"""Abstract base class shared by every HR source connector."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator

from src.models.employee import EmployeeRaw, JobApplicationRaw


class BaseConnector(ABC):
    """Abstract base for all HR source connectors."""

    @abstractmethod
    def fetch_employees(self) -> Iterator[EmployeeRaw]:
        """Yield employee records from the source system."""
        ...

    @abstractmethod
    def fetch_job_applications(self) -> Iterator[JobApplicationRaw]:
        """Yield job application records from the source system."""
        ...

    def source_name(self) -> str:
        return self.__class__.__name__.replace("Connector", "").lower()
