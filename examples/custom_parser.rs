use elysiumparser::{
    add_search_with_expression, run_parser, BooleanExpression, ParserConfig,
};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // Create search terms with complex boolean expressions
    let mut search_terms = Vec::new();
    
    // Add a search term with a primary keyword "error" and a complex boolean expression
    // This will match if the line contains "error" AND ((database & connection) OR (timeout))
    add_search_with_expression(
        &mut search_terms,
        "error",
        "(database & connection) | (timeout)",
    );
    
    // You can add multiple search terms
    add_search_with_expression(&mut search_terms, "warning", "memory");
    
    // Setup the parser configuration
    let config = ParserConfig {
        log_folder: "logs/application".to_string(),
        output_log: "logs/results.log".to_string(),
        filename_filter: "app".to_string(), // Only include files with "app" in the name
        line_filter: "".to_string(),        // No specific line filter
        search_terms,
        workers: Some(4),                   // Use 4 worker threads
    };
    
    // Define a custom progress callback
    let progress_callback = |processed: usize, total: usize| {
        println!("Processed {}/{} files ({}%)", 
            processed, 
            total, 
            (processed * 100) / total
        );
    };
    
    // Run the parser
    let result = run_parser(config, Some(progress_callback)).await?;
    
    // Use the results
    println!("Found {} matches in {} files", 
        result.total_matches,
        result.processed_files
    );
    
    Ok(())
}