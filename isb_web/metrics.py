import logging
import time
from typing import Optional

from fastapi import APIRouter, Depends
from sqlmodel import Session
from starlette.responses import PlainTextResponse

from isb_web.isb_solr_query import solr_counts_by_authority
from isb_web.sqlmodel_database import SQLModelDAO, things_by_authority_count_dict

router = APIRouter(prefix="/metrics")
dao: Optional[SQLModelDAO] = None
logging.basicConfig(level=logging.DEBUG)
_L = logging.getLogger("metrics")


def get_session():
    with dao.get_session() as session:
        yield session


class PrometheusMetrics:
    db_counts: dict[str, int]
    db_scrape_duration_seconds: float
    solr_counts: dict[str, int]
    solr_scrape_duration_seconds: float

    @staticmethod
    def _add_metrics_lines(metrics_lines: list[str], counts: dict[str, int], metric_noun: str, duration: float):
        for key, value in counts.items():
            authority_metric_name = key.lower()
            # First generate the documentation and type definition for the field
            count_field_name = f"isamples_{metric_noun}_{authority_metric_name}_count"
            metrics_lines.append(f"# HELP {count_field_name} A count of {metric_noun} from authority {key}.")
            metrics_lines.append(f"# TYPE {count_field_name} gauge")
            # Then generate the value
            metrics_lines.append(f"{count_field_name} {value}")
            metrics_lines.append("\n")
        duration_field_name = f"isamples_{metric_noun}_scrape_duration_seconds"
        metrics_lines.append(f"# HELP {duration_field_name} How long it took to do the {metric_noun} scrape.")
        metrics_lines.append(f"# TYPE {duration_field_name} gauge")
        metrics_lines.append(f"{duration_field_name} {duration}")

    def metrics_string(self) -> str:
        metrics_lines: list[str] = []
        """Returns the metrics in the prometheus format"""
        self._add_metrics_lines(metrics_lines, self.db_counts, "thing", self.db_scrape_duration_seconds)
        metrics_lines.append("\n")
        self._add_metrics_lines(metrics_lines, self.solr_counts, "solr", self.solr_scrape_duration_seconds)
        return "\n".join(metrics_lines)


def _root(session: Session):
    db_start_time = time.time()
    metrics = PrometheusMetrics()
    metrics.db_counts = things_by_authority_count_dict(session)
    db_end_time = time.time()
    metrics.db_scrape_duration_seconds = db_end_time - db_start_time
    metrics.solr_counts = solr_counts_by_authority()
    metrics.solr_scrape_duration_seconds = time.time() - db_end_time
    return PlainTextResponse(metrics.metrics_string())


# Note that prometheus seemed unhappy with /metrics/ vs. /metrics.  Include both since they should both work.
@router.get("", summary="Internal iSamples Metrics for Prometheus exporter", tags=["metrics"])
def root(session: Session = Depends(get_session)):
    return _root(session)


# Note that prometheus seemed unhappy with /metrics/ vs. /metrics.  Include both since they should both work.
@router.get("/", summary="Internal iSamples Metrics for Prometheus exporter", tags=["metrics"])
def root_with_slash(session: Session = Depends(get_session)):
    return _root(session)
