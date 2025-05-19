import argparse, json, sys
from tabulate import tabulate   # pip install tabulate for nice tables
from .models import DlcCase, State, Action, WaitReason


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="dlc")
    #dest is for naming the attribute by which subcommands can be accessed. 'required' is to indicate that a subcommand must be provided
    sp = p.add_subparsers(dest="cmd", required=True)

    #add a subcommand for new cases
    # ------------- new -------------
    newp = sp.add_parser("new", help="create a new case")
    _common_args(newp)

    #add a subcommand for updating cases
    # ------------- update ----------
    upd = sp.add_parser("update", help="update or new version")
    upd.add_argument("case_id", type=int)
    upd.add_argument("--new-version", action="store_true")
    _common_args(upd, require_any=True)

    #add a subcommand for listing cases
    # ------------- list ------------
    lst = sp.add_parser("list", help="list cases")
    lst.add_argument("--all", action="store_true", help="include inactive versions")
    return p


#This method adds common arguments for each subcommand (currently 'new', 'update', 'list') 05162025
def _common_args(pp: argparse.ArgumentParser, *, require_any=False):
    req = pp.add_argument if not require_any else pp.add_argument
    pp.add_argument("--hostname")
    pp.add_argument("--state", choices=[s.value for s in State])
    pp.add_argument("--block-dev")
    pp.add_argument("--osd-id", type=int)
    pp.add_argument("--ceph-cluster")
    pp.add_argument("--crush-weight", type=float)
    pp.add_argument("--mount")
    pp.add_argument("--action", choices=[a.value for a in Action])
    pp.add_argument("--wait-reason", choices=[w.value for w in WaitReason])


def main(argv=None):
    ns = _parser().parse_args(argv)
    if ns.cmd == "new":
        _cmd_new(ns)
    elif ns.cmd == "update":
        _cmd_update(ns)
    elif ns.cmd == "list":
        _cmd_list(ns)


def _cmd_new(ns):
    case = DlcCase(
        case_id=None,
        hostname=ns.hostname,
        state=ns.state,
        block_dev=ns.block_dev,
        osd_id=ns.osd_id,
        ceph_cluster=ns.ceph_cluster,
        crush_weight=ns.crush_weight,
        mount=ns.mount,
        action=ns.action,
    )
    case.save()
    print(f"Created case {case.case_id} (v{case.version_number})")


def _cmd_update(ns):
    try:
        case = DlcCase.load(ns.case_id)
    except ValueError as exc:
        print(exc)
        sys.exit(1)
    # overwrite provided fields
    for f in (
        "hostname",
        "state",
        "block_dev",
        "osd_id",
        "ceph_cluster",
        "crush_weight",
        "mount",
        "action",
        "wait_reason",
    ):
        val = getattr(ns, f)
        if val is not None:
            setattr(case, f, val)
        elif val is None:
            setattr(case, f, 'None')
    case.save(new_version=ns.new_version)
    action = "New version" if ns.new_version else "Updated"
    print(f"{action} case {case.case_id} (v{case.version_number})")


from .storage import db_cursor


def _cmd_list(ns):
    with db_cursor() as cur:
        sql = "SELECT * FROM dlc_case "
        if not ns.all:
            sql += "WHERE active=1 "
        sql += "ORDER BY case_id, version_number"
        cur.execute(sql)
        rows = [tuple(r) for r in cur.fetchall()]
        if not rows:
            print("No cases.")
            return
        headers = [d[0] for d in cur.description]
        print(tabulate(rows, headers=headers, tablefmt="simple"))


if __name__ == "__main__":  # so `python -m dlc.cli` works
    main()

