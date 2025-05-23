from dataclasses import dataclass, asdict, field
from enum import Enum
from typing import Optional
import sys
from .storage import db_cursor
import ceph_common as cc
import hwinv
from .miscellaneous import save_case_history
#import pickle

TABLE_NAME = "testing_table"

class State(str, Enum):
    #OSD failure -> Log failure into database -> start resolution 'Change OSD/disk attributes' (CRUSH weight, etc.) -> wait ceph health (totally clean) -> re-check OSD/disk attributes (OSD reweighted, disk not missing, etc.) -> remove OSD (osd-remove --replace) -> check failure type (IO error?) -> check smartctl -> test disk? -> test results...
    #There should be a case state plus an action for any one point in time.

    NEW = "NEW"
    NEW_DETAIL = "NEW-DETAIL"

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


#@dataclass
class DlcCase:
    def __init__(
            self,
            case_id: Optional[int] = None,
            hostname: Optional[str] = "None",
            state: Optional[State] = "NEW",
            action: Optional[Action] = "None",
            wait_reason: Optional[WaitReason] = "None",
            block_dev: str = "None",
            osd_id: int = -1,
            cluster: str = "None",
            crush_weight: float = -1.0,
            mount: str = "None",
            active: int = 1, 
            osd: Optional[cc.CephOsd] = None,
            ):
        self.case_id = case_id
        self.hostname = hostname
        self.state = state
        self.action = action
        self.wait_reason = wait_reason
        self.block_dev = block_dev
        self.osd_id = osd_id
        self.cluster = cluster
        self.crush_weight = crush_weight
        self.mount = mount
        self.active = active
        self.osd = osd

    # ---------- validation ----------
    def __post_init__(self):
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
    """
    @staticmethod
    def _next_case_id(cur):
        cur.execute(f"SELECT COALESCE(MAX(case_id), 0) + 1 FROM {TABLE_NAME}")
        return cur.fetchone()[0]
    """

    def valid_case(self):
        id_and_cluster = 0
        disk_and_hostname = 0
        
        #Took cluster out and will put back when it is deemed appropriate
        #if self.osd_id and self.cluster and self.osd_id != "None" and self.osd_id >= 0 and self.cluster != "None":
        if self.osd_id and self.osd_id != "None" and self.osd_id >= 0:
            id_and_cluster += 1
        if self.hostname and self.block_dev and self.hostname != "None" and self.block_dev != "None":
            disk_and_hostname += 1
        
        if disk_and_hostname < 1 and id_and_cluster < 1:
            return False
        elif disk_and_hostname == 1 and id_and_cluster == 1:
            return 3
        elif disk_and_hostname == 1:
            return 1
        elif id_and_cluster == 1:
            return 2

    def _check_ceph_cluster(self):
        try:
            with open ('/etc/ceph/ceph_cluster', 'r') as f:
                cluster = f.read()
                return cluster.strip(), None 
        except FileNotFoundError as e:
            return False, e

    def get_complete_information(self, valid_case):

        case = self

        #This is a way to create a dictionary out of merged dictionaries. What this says is 'object_kwargs' is my end product (the dictionary object I will use later). So build me a dictionary called 'object_kwargs' out of merged unpacked dicts. Merge the key value pairs if the condition is true, otherwise merge an empty dictionary. 
        object_kwargs = {
                **({"hostname": self.hostname} if valid_case == 1 or valid_case == 3 else {}),
                **({"block_dev": self.block_dev} if valid_case == 1 or valid_case == 3 else {}),
                **({"osd_id": self.osd_id} if valid_case == 2 or valid_case == 3 else {}),
                #**({"cluster": self.cluster} if valid_case == 2 or valid_case == 3 else {}),
            }
        #Here we're asserting that the dict is non-empty. There are other sections of the code that do this implicitly but here we do it explicitly just in case something was missed.
        assert object_kwargs

        #Here we try to find the cluster name by looking under /etc/ceph/ceph_cluster
        cluster_name, _ = self._check_ceph_cluster()

        if cluster_name:
            pass
        else:
            print("Failed to get ceph cluster name... Exiting.")
            exit(1)

        hw = hwinv.HWInv()  
        OsdMap = cc.CephOsdMap(hw)

        #Here we are iterating through the Osd Map and trying to find a matching CephOsd object
        for osd_id, osd_object in OsdMap.osd_map_local.items():

            #all() https://docs.python.org/3/library/functions.html#all is a built-in function which expresses that all conditions must be met. Returns True or False and takes an iterable, equivalent to:
            """
            def all(iterable):
                for element in iterable:
                    if not element:
                    return False
                return True
            """
            #getattr(object, name, default) https://docs.python.org/3/library/functions.html#getattr Returns the value of the named attribute of an object. The name must be a string, if a default return value is not provided and the named attribute doesn't exist, an AttributeError is raised.
            #So this says return True if ALL attribute values in CephOsd are equivalent to their corresponding dictionary values in object_kwargs, otherwise return False.
            found_osd_equivalent = all(
                getattr(osd_object, key) == value
                for key, value in object_kwargs.items()
                #Currently omitting cluster as CephOsd object doesn't include it but we can add it back
                if key != 'cluster'
            )

            #Here we are instantiating a DlcCase object if we find an OSD in the local portion of the OSD Map. This needs to be changed so that we look at the whole map when the state is "NEW", and look at the local portion if the case is in any other state.
            if found_osd_equivalent:
                print("Found an equivalent OSD in the local region of the OSD Map (This host).")
                case = DlcCase(
                    case_id=None,
                    hostname=osd_object.hostname,
                    block_dev=str(osd_object.dev_name),
                    osd_id=osd_id,
                    #cluster name from local host instead of cluster fsid from CephOsd
                    cluster=cluster,
                    crush_weight=osd_object.crush_weight,
                    mount=osd_object.lv_name,
                    action="logging info",
                    wait_reason="None",
                    osd=osd_object
                )
                if case.state == "NEW":
                    case.state="NEW-DETAILS"
                break

        #This may need to be changed. According to Andras' instructions, if a corresponding OSD object is not found then we should not create or update a case. There is a check for this condition in the 'save' method and that is why we return the value of 'found_osd_equivalent'.
        if found_osd_equivalent == False:
            case.osd = cc.CephOsd(case.osd_id)
        return case, found_osd_equivalent

    #This validates the case by checking that osd_id is an int and is positive and that crush weight is positive.
    def _validate_case(self):
        _validate_positive_int(self.osd_id, "osd_id")
        if self.crush_weight < 0:
            raise ValueError("crush weight must be ≥ 0")
        return True

    #This * means that any arguments after it will not be positional and will be supplied as keyword arguments
    def save(self, *, new_version: bool = False, force_save: bool = False):

        case = self

        #if disk and hostname return value is 1, osd_id and cluster return value is 2. If all 4 are present, return value is 3
        #I took out cluster from the list of available arguments for an operator, I can put this back when it is appropriate. For now, assuming that the cluster name is the same as that which is listed under the local /etc/ceph/ceph_cluster
        valid_case = self.valid_case()

        if valid_case:
            pass
        else:
            print("In order to save a case you must provide either an osd id or a block device and hostname. Exiting...")
            sys.exit(1)

        #if self.state == "NEW" and found_osd_equivalent:
        case, found_osd_equivalent = self.get_complete_information(valid_case)
        #case.state = "NEW-DETAILS"
        #_ , found_osd_equivalent = self.get_complete_information(valid_case)

        try:
            self._validate_case()
        except ValueError as e:
            print(e)
            sys.exit(1)

        #Here we're going to save if we found an OSD candidate in the OSD map or if the user is forcing us to try.
        if force_save == True or found_osd_equivalent == True:
            with db_cursor() as cur:
                data = {
                    "hostname": case.hostname,
                    "state": case.state,
                    "block_dev": case.block_dev,
                    "osd_id": case.osd_id,
                    "cluster": case.cluster,
                    "crush_weight": case.crush_weight,
                    "mount": case.mount,
                    "active": case.active,
                    "action": case.action,
                    "wait_reason": case.wait_reason
                }

                if new_version:
                    #save history
                    print("Going into history to save...")
                    save_case_history(case.case_id)
                    set_clause = ", ".join([f"{key} = :{key}" for key in data.keys()])
                    cur.execute(
                        f"UPDATE {TABLE_NAME} SET {set_clause} WHERE case_id = :case_id",
                        {**data, "case_id": case.case_id},
                    )
                else:
                    columns = ", ".join(data.keys())
                    placeholders = ", ".join([f":{key}" for key in data.keys()])
                    cur.execute(
                        f"INSERT INTO {TABLE_NAME} ({columns}) VALUES ({placeholders})",
                        data,
                    )
                    case.case_id = cur.lastrowid

            return case

        else:
            print("Did not find an equivalent OSD in the OSD map and force_save=False, so nothing was saved.")
            return False

    @staticmethod
    def load(case_id: int, version: Optional[int] = None) -> "DlcCase":
        sql = f"SELECT * FROM {TABLE_NAME} WHERE case_id=? "
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

    def progress(self):

        if self.state == "NEW-DETAILS":
            case, found_osd_equivalent = self.get_complete_information(3)
            #Check OSD properties... if it seems like a normal OSD found_osd_equivalent == True
            #Then we're going to do things like:

            #1. Ensure Daemon is stopped
            
            #2. Check and  Change CRUSH weight to 0
            #self.crush_weight = 0
            
            #3. Put OSD and other info into database
            #self.save(new_version = True)
            
            #4. Watch for Ceph recovery
            #Here if ceph doesn't report health ok then we stop until this program again scans for active cases that need updating or a user manually activates an update for the case

            #5. Remove OSD and prep for testing or replacement
            
            #ceph_admin.osd_remove already has all the commands but doesn't wait for cluster health after taking the OSD out. So, need to either rewrite that code or just paste parts of it here.
            #Then progress to state "RECOVERY_WAIT"
            #in ceph_common there is a class called CephState and there is a method called is_clean
            self.state = "RECOVERY_WAIT"
            #state = cc.CephState()
            #state.is_clean() returns True or False
            #

    # convenience
    def as_row(self):
        d = asdict(self)
        d["state"] = d["state"].value
        return d

