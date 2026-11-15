package naming

import "testing"

func TestDatasetAndSnapshotNames(t *testing.T) {
	if got := Dataset("tank/k8s/", "/pvc-123"); got != "tank/k8s/pvc-123" {
		t.Fatalf("Dataset = %q", got)
	}
	if got := SnapshotHandle("pvc-123", "vs-abc"); got != "pvc-123@vs-abc" {
		t.Fatalf("SnapshotHandle = %q", got)
	}
	v, s, err := SplitSnapshotHandle("pvc-123@vs-abc")
	if err != nil || v != "pvc-123" || s != "vs-abc" {
		t.Fatalf("split = %q %q %v", v, s, err)
	}
	for _, bad := range []string{"", "@", "foo", "foo@", "@bar"} {
		if _, _, err := SplitSnapshotHandle(bad); err == nil {
			t.Fatalf("SplitSnapshotHandle(%q) should fail", bad)
		}
	}
	if got := SnapshotName("vs", "a/b c@d"); got != "vs-a-b-c-d" {
		t.Fatalf("sanitized name = %q", got)
	}
	if !ValidDataset("tank/k8s/pvc-1") || ValidDataset("") || ValidDataset("/tank/x") || ValidDataset("tank/../x") {
		t.Fatal("ValidDataset returned wrong result")
	}
	if !ValidSnapshot("vs-1") || ValidSnapshot("a/b") || ValidSnapshot("a@b") {
		t.Fatal("ValidSnapshot returned wrong result")
	}
}
