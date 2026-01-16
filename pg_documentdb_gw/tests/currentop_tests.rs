pub mod common;
use mongodb::bson::doc;

#[tokio::test]
async fn test_currentop_command_works() {
    let db = common::initialize_with_db("currentop_test").await;
    
    let result = db.run_command(doc! {"currentOp": 1}).await.unwrap();
    
    // Verify currentOp returns expected structure
    assert!(result.contains_key("ok"), "Response should have 'ok' field");
    assert!(result.contains_key("inprog"), "Response should have 'inprog' field");
    
    let ok_value = result.get_f64("ok").unwrap();
    assert_eq!(ok_value, 1.0, "Expected ok to be 1.0");
    
    let inprog = result.get_array("inprog").unwrap();
    assert!(!inprog.is_empty(), "Expected inprog to contain operations");
}

#[tokio::test]
async fn test_currentop_shows_active_field() {
    let db = common::initialize_with_db("currentop_active_test").await;
    
    let result = db.run_command(doc! {"currentOp": 1}).await.unwrap();
    
    let inprog = result.get_array("inprog").unwrap();
    assert!(!inprog.is_empty(), "Expected inprog to contain operations");
    
    // Verify all operations have 'active' field
    for (i, op) in inprog.iter().enumerate() {
        if let Some(doc) = op.as_document() {
            assert!(
                doc.contains_key("active"),
                "Operation {i} missing 'active' field"
            );
        }
    }
}

#[tokio::test]
async fn test_currentop_with_all_users() {
    let db = common::initialize_with_db("currentop_all_users_test").await;
    
    let result = db.run_command(doc! {"currentOp": 1, "$all": true}).await.unwrap();
    
    assert!(result.contains_key("ok"), "Response should have 'ok' field");
    assert!(result.contains_key("inprog"), "Response should have 'inprog' field");
    
    let ok_value = result.get_f64("ok").unwrap();
    assert_eq!(ok_value, 1.0, "Expected ok to be 1.0");
    
    let inprog = result.get_array("inprog").unwrap();
    assert!(!inprog.is_empty(), "Expected inprog to contain operations");
}

#[tokio::test]
async fn test_currentop_with_idle_sessions() {
    let db = common::initialize_with_db("currentop_idle_test").await;
    
    let result = db.run_command(doc! {"currentOp": 1, "idleSessions": true}).await.unwrap();
    
    assert!(result.contains_key("ok"), "Response should have 'ok' field");
    assert!(result.contains_key("inprog"), "Response should have 'inprog' field");
    
    let ok_value = result.get_f64("ok").unwrap();
    assert_eq!(ok_value, 1.0, "Expected ok to be 1.0");
}
