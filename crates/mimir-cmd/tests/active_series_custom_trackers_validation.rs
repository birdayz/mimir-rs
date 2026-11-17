//! Go: pkg/ingester/activeseries/model/custom_trackers_config.go — invalid custom tracker matchers
//! fail config loading (`UnmarshalYAML`) and flag parsing (`Set`) instead of being ignored.

use std::fs;

use mimir_cmd::run_cli_test_mode;

fn run(args: &[&str]) -> mimir_cmd::CliOutcome {
    let args: Vec<String> = args.iter().map(|s| (*s).to_string()).collect();
    run_cli_test_mode("./mimir", &args)
}

#[test]
fn invalid_yaml_custom_tracker_matchers_fail_config_loading() {
    let dir = tempfile::tempdir().unwrap();
    for key in ["active_series_custom_trackers", "active_series_additional_custom_trackers"] {
        let path = dir.path().join(format!("{key}.yaml"));
        fs::write(&path, format!("limits:\n  {key}:\n    bad: 'not a matcher'\n")).unwrap();
        let out = run(&["-target=ingester", &format!("-config.file={}", path.display())]);
        assert_eq!(out.exit_code, 1, "{key}: {out:?}");
        assert!(
            out.stderr
                .contains("can't build active series matcher bad: bad matcher format: not a matcher"),
            "{key}: {}",
            out.stderr
        );
    }

    let ok = dir.path().join("ok.yaml");
    fs::write(
        &ok,
        "limits:\n  active_series_custom_trackers:\n    good: '{team=\"a\"}'\n  active_series_additional_custom_trackers:\n    more: '{team=~\"b.*\"}'\n",
    )
    .unwrap();
    let out = run(&["-target=ingester", &format!("-config.file={}", ok.display())]);
    assert_eq!(out.exit_code, 0, "{}", out.stderr);
}

#[test]
fn invalid_custom_tracker_flag_values_exit_2_with_go_messages() {
    let cases = [
        (
            "foo:{bad",
            "invalid value \"foo:{bad\" for flag -ingester.active-series-custom-trackers: can't build active series matcher foo: bad matcher format: bad",
        ),
        (
            "foo",
            "invalid value \"foo\" for flag -ingester.active-series-custom-trackers: value should be <name>:<matcher>[;<name>:<matcher>]*, but colon was not found in the value 0: \"foo\"",
        ),
        (
            "a:{x=\"1\"};a:{x=\"2\"}",
            "matcher \"a\" for active series custom trackers is provided twice",
        ),
    ];
    for (value, want) in cases {
        let flag = format!("-ingester.active-series-custom-trackers={value}");
        let out = run(&["-target=ingester", &flag]);
        assert_eq!(out.exit_code, 2, "{value}: {out:?}");
        assert!(out.stderr.contains(want), "{value}: {}", out.stderr);
    }
}
