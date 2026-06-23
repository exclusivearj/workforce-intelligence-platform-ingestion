"""Local Flask server that mimics paginated Workday + Greenhouse REST APIs.

Serves synthetic data (from src.utils.synthetic_data) using source-native field
names and a ``_links.next`` pagination contract, so the WorkdayConnector and
GreenhouseConnector exercise a real HTTP stack rather than mocked responses:

* ``/api/v1/workers``       — Workday workers (employees)
* ``/api/v1/applications``  — Greenhouse job applications

A single process backs both so the Airflow DAG can point WORKDAY_BASE_URL and
GREENHOUSE_BASE_URL at one mock service.

Run: ``python docker/mock_workday_server.py`` (listens on :5001).
"""

from __future__ import annotations

from flask import Flask, jsonify, request

from src.utils.synthetic_data import generate_employees, generate_job_applications

app = Flask(__name__)

# Generate once at startup so pagination is stable across requests.
_EMPLOYEES = generate_employees(500, seed=42)
_APPLICATIONS = generate_job_applications(_EMPLOYEES, 1000, seed=42)

# Reverse of GREENHOUSE_STATUS_MAP: canonical stage -> Greenhouse-native status,
# so the GreenhouseConnector maps it back to the same stage enum.
_STAGE_TO_GH_STATUS = {
    "applied": "submitted",
    "phone_screen": "phone_screen",
    "interview": "interviewing",
    "offer": "offer_extended",
    "hired": "hired",
    "rejected": "rejected",
}


def _to_workday(emp: dict) -> dict:
    """Translate a synthetic employee dict into Workday API field names."""
    return {
        "Worker_ID": emp["source_id"],
        "Legal_First_Name": emp["first_name"],
        "Legal_Last_Name": emp["last_name"],
        "Work_Email": emp["email"],
        "Cost_Center": emp["department"],
        "Job_Title": emp["job_title"],
        "Hire_Date": emp["hire_date"],
        "Termination_Date": emp["termination_date"],
        "Worker_Type": emp["employment_type"],
        "Management_Level": emp["level"],
        "Location": emp["location"],
        "Manager_ID": emp["manager_id"],
        "Base_Pay": emp["salary"],
        "Performance_Rating": emp["performance_rating"],
    }


@app.route("/api/v1/workers")
def get_workers():
    page = int(request.args.get("page", 1))
    page_size = int(request.args.get("page_size", 100))
    start = (page - 1) * page_size
    end = start + page_size
    chunk = _EMPLOYEES[start:end]

    body = {"data": [_to_workday(e) for e in chunk], "_links": {}}
    if end < len(_EMPLOYEES):
        body["_links"]["next"] = f"/api/v1/workers?page={page + 1}&page_size={page_size}"
    return jsonify(body)


@app.route("/api/v1/workers/<worker_id>")
def get_worker(worker_id: str):
    for emp in _EMPLOYEES:
        if emp["source_id"] == worker_id:
            return jsonify(_to_workday(emp))
    return jsonify({"error": "not found"}), 404


def _to_greenhouse(app_rec: dict) -> dict:
    """Translate a synthetic application dict into Greenhouse API field names."""
    return {
        "id": app_rec["source_id"],
        "candidate_id": app_rec["candidate_id"],
        "job_id": app_rec["job_id"],
        "job_title": app_rec["job_title"],
        "department": app_rec["department"],
        "status": _STAGE_TO_GH_STATUS.get(app_rec["stage"], "submitted"),
        "applied_at": app_rec["applied_at"],
        "last_activity_at": app_rec["stage_changed_at"],
        "recruiter_id": app_rec["recruiter_id"],
    }


@app.route("/api/v1/applications")
def get_applications():
    page = int(request.args.get("page", 1))
    page_size = int(request.args.get("page_size", 100))
    start = (page - 1) * page_size
    end = start + page_size
    chunk = _APPLICATIONS[start:end]

    body = {"data": [_to_greenhouse(a) for a in chunk], "_links": {}}
    if end < len(_APPLICATIONS):
        body["_links"]["next"] = (
            f"/api/v1/applications?page={page + 1}&page_size={page_size}"
        )
    return jsonify(body)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=False)
