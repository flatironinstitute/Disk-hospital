from dataclasses import dataclass, asdict, field
from enum import Enum
from typing import Optional

from .storage import db_cursor


class State(str, Enum):
    #OSD failure -> Log failure into database -> start resolution 'Change OSD/disk attributes' (CRUSH weight, etc.) -> wait ceph health (totally clean) -> re-check OSD/disk attributes (OSD reweighted, disk not missing, etc.) -> remove OSD (osd-remove --replace) -> check failure type (IO error?) -> check smartctl -> test disk? -> test results...
    #There should be a case state plus an action for any one point in time.

    DETECTED = "Issue detected"
    INVESTIGATING = "Investigating"
    TAKING_ACTION = "Taking action"
    WAITING = "Waiting"
    WITH_OPERATOR = "With operator"
    CLOSED = "Closed"
    CLOSED_RESOLVED = "Resolved"

class Action(str, Enum):
    logging = "Logging info"
    testing_disk = "Testing disk"
    checking_info = "Checking for information"
    checking_smart = "Checking SMART Health"
    editing_OSD = "Editing OSD"
    operator_handoff = "Handing to operator"
    removing_OSD = "Removing OSD"
    reweighting_OSD = "Reweighting OSD"
    none = "None"

class WaitReason(str, Enum):
    cluster_health = "Waiting for 'HEALTH_OK'"
    disk_test_completion = "Waiting for disk test to finish"
    disk_replacement_completion = "Waiting for disk replacement"
    none = "None"

def _validate_positive_int(value: int, name: str):
    if not isinstance(value, int) or value < 0:
        raise ValueError(f"{name} must be a non-negative integer")


@dataclass
class DlcCase:
    case_id: Optional[int]
    hostname: str
    state: State
    action: Action
    wait_reason: WaitReason
    block_dev: str
    osd_id: int
    ceph_cluster: str
    crush_weight: float
    mount: str
    version_number: int = 1
    # version_date_time is DB-generated; keep placeholder
    version_date_time: str = field(default="<db-filled>")
    #I don't see why this necessarily should always be 1. I should be able to create a new case 'state' or 'version' which is inactive at the get-go. But this is more to indicate which version is more current. 'State' is used to indicate the state of the case
    active: int = field(default=1, init=False)

    # ---------- validation ----------
    def __post_init__(self):
        _validate_positive_int(self.osd_id, "osd_id")
        if self.crush_weight < 0:
            raise ValueError("crush_weight must be ≥ 0")
        if not isinstance(self.state, State):
            # argparse passes strings → cast
            try:
                self.state = State(self.state)
            except ValueError as e:
                raise ValueError(
                    f"state must be one of {[s.value for s in State]}"
                ) from e

    # ---------- DB helpers ----------
    #This doesn't need an automatic argument such as self or cls. It's a standalone helper function that is here for organizational purposes. @staticmethod is an indicator that this is the case. The underscore at the beginning means that this is a private functions to be used only within the class
    @staticmethod
    def _next_case_id(cur):
        cur.execute("SELECT COALESCE(MAX(case_id), 0) + 1 FROM dlc_case")
        return cur.fetchone()[0]

    #This * means that any arguments after it will not be positional and will be supplied as keyword arguments
    def save(self, *, new_version: bool = False) -> None:
        with db_cursor() as cur:
            if self.case_id is None:
                self.case_id = self._next_case_id(cur)
            elif new_version:
                cur.execute(
                    "UPDATE dlc_case SET active=0 "
                    "WHERE case_id=? AND active=1",
                    (self.case_id,),
                )
                self.version_number += 1

            try:
                cur.execute(
                    """
                    INSERT INTO dlc_case (
                        case_id, hostname, state, block_dev, osd_id,
                        ceph_cluster, crush_weight, mount,
                        version_number, active, action, wait_reason
                    )
                    VALUES (:case_id, :hostname, :state, :block_dev, :osd_id,
                            :ceph_cluster, :crush_weight, :mount,
                            :version_number, :active, :action, :wait_reason)
                    """,
                    asdict(self),
                )
            except sqlite3.IntegrityError as e:
                print("Disk hospital unable to save/update case due to an integrity error:", e)
                exit(1)


    @staticmethod
    def load(case_id: int, version: Optional[int] = None) -> "DlcCase":
        sql = "SELECT * FROM dlc_case WHERE case_id=? "
        params = [case_id]
        if version is None:
            sql += "AND active=1"
            pass
        else:
            sql += "AND version_number=?"
            params.append(version)

        with db_cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            if row is None:
                raise ValueError("Case not found")
            #Dropping 'rowid' and 'active' columns from the query as it's not present in or relevant to the object
            row_dict = dict(row)
            row_dict.pop("rowid", None)
            row_dict.pop("active", None)
            #print(DlcCase)
            return DlcCase(**row_dict)

    # convenience
    def as_row(self):
        d = asdict(self)
        d["state"] = d["state"].value
        return d

