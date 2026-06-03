package option

import "testing"

func TestAwgOptionsIsAvailbleDetectsConfiguredParameters(t *testing.T) {
	if (AwgOptions{}).IsAvailble() {
		t.Fatal("empty AWG options should not be available")
	}

	if !(AwgOptions{Jc: 1}).IsAvailble() {
		t.Fatal("numeric AWG option should be available")
	}

	if !(AwgOptions{H1: "host"}).IsAvailble() {
		t.Fatal("string AWG option should be available")
	}
}

func TestAwgEndpointOptionsEffectiveAwgOptionsPrefersNestedOptions(t *testing.T) {
	options := AwgEndpointOptions{
		Jc: 1,
		Awg: AwgOptions{
			Jc: 9,
			H1: "nested",
		},
	}

	effective := options.EffectiveAwgOptions()
	if effective.Jc != 9 || effective.H1 != "nested" {
		t.Fatalf("expected nested AWG options, got %+v", effective)
	}
}

func TestAwgEndpointOptionsEffectiveAwgOptionsFallsBackToFlatOptions(t *testing.T) {
	options := AwgEndpointOptions{
		Jc: 3,
		H1: "flat",
		I5: "i5",
	}

	effective := options.EffectiveAwgOptions()
	if effective.Jc != 3 || effective.H1 != "flat" || effective.I5 != "i5" {
		t.Fatalf("expected flat AWG options, got %+v", effective)
	}
}
