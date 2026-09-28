use serde_json::json;

use super::{
    InstallState, PermissionErrorKind, canopy_tool_name, computer_use_tool_names,
    detect_permission_error, install_state_from_json, install_state_to_json,
};

#[test]
fn permission_detection_matches_driver_error_categories_and_priority() {
    let no_error =
        json!({"isError": false, "content": [{"type":"text", "text":"Accessibility missing"}]});
    assert_eq!(
        detect_permission_error(&no_error),
        PermissionErrorKind::None
    );

    let accessibility = json!({"isError": true, "content": [{"type":"text", "text":"❌ Accessibility: NOT granted."}]});
    assert_eq!(
        detect_permission_error(&accessibility),
        PermissionErrorKind::Accessibility
    );

    let both = json!({"isError": true, "content": [{"type":"text", "text":"Screen Recording missing; Accessibility permission missing"}]});
    assert_eq!(
        detect_permission_error(&both),
        PermissionErrorKind::Accessibility
    );

    let generic = json!({"isError": true, "content": [{"type":"text", "text":"Missing TCC grant(s) for this process."}]});
    assert_eq!(
        detect_permission_error(&generic),
        PermissionErrorKind::UnknownPermission
    );

    let unrelated = json!({"isError": true, "content": [{"type":"text", "text":"appNotFound"}]});
    assert_eq!(
        detect_permission_error(&unrelated),
        PermissionErrorKind::Other
    );
}

#[test]
fn install_state_is_shape_checked_and_requires_exact_approval_match() {
    assert_eq!(install_state_from_json("not-json"), None);
    assert_eq!(
        install_state_from_json(r#"{"approvedPackageSpec": 42}"#),
        None
    );

    let state = install_state_from_json(
        r#"{"approvedPackageSpec":"cua-driver-rs@0.5.2","approvedAtIso":"2026-05-28T10:00:00Z","extra":true}"#,
    )
    .unwrap();
    assert!(state.approves("cua-driver-rs@0.5.2"));
    assert!(!state.approves("cua-driver-rs@0.6.0"));

    let round_trip = install_state_from_json(&install_state_to_json(&state).unwrap()).unwrap();
    assert_eq!(round_trip, state);
}

#[test]
fn pinned_tool_catalog_uses_computer_use_prefixed_names_at_registration_boundary() {
    assert!(computer_use_tool_names().iter().any(|name| name == "click"));
    assert_eq!(
        canopy_tool_name("click").as_deref(),
        Some("computer_use__click")
    );
}

#[test]
fn install_state_struct_matches_the_persisted_wire_names() {
    let encoded = install_state_to_json(&InstallState {
        approved_package_spec: "cua-driver-rs@0.5.2".to_owned(),
        approved_at_iso: "2026-05-28T10:00:00Z".to_owned(),
    })
    .unwrap();
    assert!(encoded.contains("approvedPackageSpec"));
    assert!(encoded.contains("approvedAtIso"));
}
