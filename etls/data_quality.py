from __future__ import annotations

from dataclasses import asdict, dataclass, field

import pandas as pd

from utils.constants import ERROR_HANDLING, MIN_ROWS, REQUIRED_COLUMNS
from utils.logger import get_logger

log = get_logger(__name__)


class DataQualityError(RuntimeError):


@dataclass
class QualityReport:
    rows_in: int = 0
    rows_out: int = 0
    errors: list = field(default_factory=list)
    warnings: list = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return not self.errors

    def to_dict(self) -> dict:
        data = asdict(self)
        data['passed'] = self.passed
        return data


def validate_posts(df: pd.DataFrame, error_handling: str | None = None,
                   min_rows: int | None = None, now=None):
    mode = (error_handling or ERROR_HANDLING).lower()
    min_rows = MIN_ROWS if min_rows is None else min_rows
    now = pd.Timestamp(now) if now is not None else pd.Timestamp.now(tz='UTC').tz_localize(None)
    report = QualityReport(rows_in=len(df))

    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        report.errors.append(f'missing required columns: {missing}')
        return _finish(df, report, mode)

    if len(df) < min_rows:
        report.errors.append(f'only {len(df)} rows, expected at least {min_rows}')

    clean = df.copy()

    bad_id = clean['id'].isna() | (clean['id'].astype(str).str.strip() == '')
    if bad_id.any():
        report.errors.append(f'{int(bad_id.sum())} rows without an id')
    clean = clean[~bad_id]

    dupes = clean['id'].duplicated(keep='first')
    if dupes.any():
        report.errors.append(f'{int(dupes.sum())} duplicate ids')
    clean = clean[~dupes]

    future = clean['created_utc'] > now + pd.Timedelta(hours=1)
    if future.any():
        report.errors.append(f'{int(future.sum())} posts created in the future')
    clean = clean[~future]

    if clean['title'].astype(str).str.strip().eq('').any():
        report.warnings.append(f"{int(clean['title'].astype(str).str.strip().eq('').sum())} empty titles")
    if (clean['score'] < 0).any():
        report.warnings.append(f"{int((clean['score'] < 0).sum())} posts with negative score")
    if (clean['num_comments'] < 0).any():
        report.warnings.append('negative comment counts found')
    if 'upvote_ratio' in clean.columns:
        out_of_range = clean['upvote_ratio'].dropna()
        out_of_range = out_of_range[(out_of_range < 0) | (out_of_range > 1)]
        if len(out_of_range):
            report.warnings.append(f'{len(out_of_range)} upvote_ratio values outside [0, 1]')
    if clean['url'].isna().any():
        report.warnings.append(f"{int(clean['url'].isna().sum())} posts without url")
    if clean['created_utc'].isna().any():
        report.warnings.append(f"{int(clean['created_utc'].isna().sum())} posts without created_utc")

    return _finish(clean, report, mode)


def _finish(df: pd.DataFrame, report: QualityReport, mode: str):
    report.rows_out = len(df)
    for warning in report.warnings:
        log.warning('Data quality warning: %s', warning)
    if report.errors:
        for error in report.errors:
            log.error('Data quality error: %s', error)
        if mode == 'abort':
            raise DataQualityError('; '.join(report.errors))
        log.warning("error_handling=%s -> continuing with %d/%d rows",
                    mode, report.rows_out, report.rows_in)
    else:
        log.info('Data quality passed (%d rows)', report.rows_out)
    return df.reset_index(drop=True), report
