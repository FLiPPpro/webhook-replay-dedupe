import io
import os
import sys
import unittest
from contextlib import redirect_stdout

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)
import replaycheck as rc  # noqa: E402

S = os.path.join(ROOT, "samples")


def run(argv):
    buf = io.StringIO()
    with redirect_stdout(buf):
        code = rc.main(argv)
    return code, buf.getvalue()


class ReplayCheck(unittest.TestCase):
    def test_keys_prefer_provider_ids(self):
        self.assertEqual(rc.dedupe_key({"headers": {"X-GitHub-Delivery": "g1"}, "body": {}})[0], "x-github-delivery:g1")
        self.assertEqual(rc.dedupe_key({"headers": {}, "body": '{"id":"evt_9"}'})[0], "stripe:evt_9")
        self.assertEqual(rc.dedupe_key({"headers": {}, "body": {"event_id": "Ev1"}})[0], "slack:Ev1")
        self.assertTrue(rc.dedupe_key({"headers": {}, "body": {"a": 1}})[0].startswith("sha256:"))

    def test_double_execution_detected(self):
        code, out = run(["audit", "--deliveries", S + "/deliveries.jsonl", "--effects", S + "/effects.jsonl",
                         "--business-key", "data.object.customer,data.object.amount"])
        self.assertEqual(code, 1)
        self.assertIn("DOUBLE  stripe:evt_1Q9A  side effect ran 2x", out)
        self.assertIn("SAME-ACTION  ['cus_12', 12000]", out)
        self.assertIn("RAN 1x", out)  # the GitHub redelivery was deduplicated correctly

    def test_clean_log_exits_zero(self):
        code, out = run(["audit", "--deliveries", S + "/clean.jsonl"])
        self.assertEqual(code, 0)
        self.assertIn("VERDICT CLEAN", out)

    def test_unreadable_is_never_zero(self):
        code, out = run(["audit", "--deliveries", S + "/broken.jsonl"])
        self.assertEqual(code, 2)
        self.assertIn("VERDICT UNKNOWN", out)

    def test_simulated_race(self):
        code, out = run(["simulate"])
        self.assertEqual(code, 0)
        self.assertIn("check-then-act  charges=2", out)
        self.assertIn("atomic-claim    charges=1", out)


if __name__ == "__main__":
    unittest.main()
