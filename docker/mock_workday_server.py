"""Local Flask server that mimics a paginated Workday REST API.

Serves synthetic data (from src.utils.synthetic_data) using Workday-style field
names and a ``_links.next`` pagination contract, so the WorkdayConnector exercises
a real HTTP stack rather than mocked responses.

Run: ``python docker/mock_workday_server.py`` (listens on :5001).
"""

from __future__ import annotations

from flask import Flask, jsonify, request

from src.utils.synthetic_data import generate_employees

app = Flask(__name__)

# Generate once at startup so pagination is stable across requests.
_EMPLOYEES = generate_employees(500, seed=42)


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


if __name__ == "__main__":
    app.run(port=5001, debug=False)
