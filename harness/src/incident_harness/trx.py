from pathlib import Path
from xml.etree import ElementTree


_NS = {"trx": "http://microsoft.com/schemas/VisualStudio/TeamTest/2010"}


def parse_trx(path: Path) -> dict:
    root = ElementTree.parse(path).getroot()
    summary = root.find("trx:ResultSummary/trx:Counters", _NS)
    if summary is None:
        raise ValueError("TRX has no result counters")

    counters = {
        name: int(summary.attrib[name])
        for name in ("total", "executed", "passed", "failed", "notExecuted")
    }
    definitions = {}
    for definition in root.findall("trx:TestDefinitions/trx:UnitTest", _NS):
        method = definition.find("trx:TestMethod", _NS)
        if method is None:
            raise ValueError("TRX test definition has no method")
        identity = f"{method.attrib['className']}.{method.attrib['name']}"
        test_id = definition.attrib["id"]
        if test_id in definitions or identity in definitions.values():
            raise ValueError("TRX contains duplicate test identities")
        definitions[test_id] = identity

    tests = []
    result_ids = set()
    for result in root.findall("trx:Results/trx:UnitTestResult", _NS):
        test_id = result.attrib["testId"]
        if test_id not in definitions:
            raise ValueError("TRX result has no matching definition")
        if test_id in result_ids:
            raise ValueError("TRX contains duplicate results")
        result_ids.add(test_id)
        outcome = result.attrib["outcome"]
        if outcome not in {"Passed", "Failed", "NotExecuted"}:
            raise ValueError(f"Unsupported TRX outcome: {outcome}")
        tests.append({"identity": definitions[test_id], "outcome": outcome})

    observed = {
        "total": len(tests),
        "executed": sum(test["outcome"] != "NotExecuted" for test in tests),
        "passed": sum(test["outcome"] == "Passed" for test in tests),
        "failed": sum(test["outcome"] == "Failed" for test in tests),
        "notExecuted": sum(test["outcome"] == "NotExecuted" for test in tests),
    }
    if set(definitions) != result_ids or counters != observed:
        raise ValueError("TRX counters, definitions, and results disagree")
    return {"counters": counters, "tests": tests}
