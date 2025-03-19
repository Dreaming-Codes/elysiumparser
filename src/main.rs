use clap::Parser;
use flate2::read::GzDecoder;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

const BAR_LENGTH: usize = 100;
const BAR_FILLER: &str = "-";
const NO_RESULT: &str = "x";

#[derive(Parser)]
#[command(author, version, about = "Log file parser")]
struct Cli {
    /// Directory containing log files to parse
    #[arg(short, long, default_value = "logs/parser")]
    log_folder: String,

    /// Output log file path
    #[arg(short, long, default_value = "logs/parser/output.log")]
    output_log: String,

    /// Filter for filenames (case insensitive)
    #[arg(short, long, default_value = "")]
    filename_filter: String,

    /// Filter for line content (case insensitive)
    #[arg(short = 'L', long, default_value = "")]
    line_filter: String,

    /// Search terms
    #[arg(short, long)]
    search: Vec<String>,

    /// Additional search terms (AND condition with primary terms)
    #[arg(short, long)]
    additional: Vec<String>,
}

struct SearchTerm {
    keyword: String,
    additional_keyword: String,
}

fn main() {
    let mut cli = Cli::parse();
    let mut search_terms = Vec::new();
    
    // Process search terms
    if cli.search.is_empty() && cli.additional.is_empty() {
        // Default search term if none provided
        add_search(&mut search_terms, "", "Master");
    } else {
        // Pad the shorter vector with empty strings
        let max_len = cli.search.len().max(cli.additional.len());
        cli.search.resize(max_len, String::new());
        cli.additional.resize(max_len, String::new());
        
        // Create search terms from command line arguments
        for i in 0..max_len {
            add_search(&mut search_terms, &cli.search[i], &cli.additional[i]);
        }
    }
    
    // Convert filters to lowercase
    let filename_filter = cli.filename_filter.to_lowercase();
    let line_filter = cli.line_filter.to_lowercase();
    
    // Initialize and print header
    println!("LOG Parser 1.0");
    println!("--------------");
    println!("Filters:");
    println!(" Filename: [{}]", filename_filter);
    println!(" Line: [{}]", line_filter);
    println!();
    
    print!("Searching for: ");
    for term in &search_terms {
        print!("[{}", term.keyword);
        if !term.additional_keyword.is_empty() {
            print!(" + {}", term.additional_keyword);
        }
        print!("] ");
    }
    println!();
    println!();
    
    // Initialize output file
    if Path::new(&cli.output_log).exists() {
        fs::remove_file(&cli.output_log).expect("Failed to delete existing output log");
    }
    
    let log_dir = Path::new(&cli.log_folder);
    if !log_dir.exists() {
        fs::create_dir_all(log_dir).expect("Failed to create log directory");
    }
    
    let mut output_file = OpenOptions::new()
        .write(true)
        .create(true)
        .open(&cli.output_log)
        .expect("Failed to create output file");
    
    // Process files
    let mut match_count = 0;
    
    // Process all files in the log folder
    match fs::read_dir(&cli.log_folder) {
        Ok(entries) => {
            for entry in entries {
                if let Ok(entry) = entry {
                    let path = entry.path();
                    
                    if is_valid_log_file(&path, &filename_filter, &cli.output_log) {
                        // Process regular log file
                        let file_match_count = process_file(
                            &path,
                            &search_terms,
                            &line_filter,
                            &mut output_file,
                        );
                        match_count += file_match_count;
                    } else if is_gz_file(&path) && path.to_string_lossy().to_lowercase().contains(&filename_filter) {
                        // Process gzipped file
                        match process_gz_file(
                            &path,
                            &search_terms,
                            &line_filter,
                            &mut output_file,
                        ) {
                            Ok(gz_match_count) => match_count += gz_match_count,
                            Err(e) => eprintln!("Error processing gzip file {}: {}", path.display(), e),
                        }
                    }
                }
            }
        }
        Err(e) => eprintln!("Error reading log directory: {}", e),
    }
    
    // Print summary
    println!();
    println!("Total occurrencies: {}", match_count);
}

fn add_search(search_terms: &mut Vec<SearchTerm>, keyword: &str, additional_keyword: &str) {
    search_terms.push(SearchTerm {
        keyword: keyword.to_lowercase(),
        additional_keyword: additional_keyword.to_lowercase(),
    });
}

fn is_valid_log_file(path: &PathBuf, filename_filter: &str, output_log: &str) -> bool {
    if !path.is_file() {
        return false;
    }
    
    if let Some(extension) = path.extension() {
        if extension != "log" {
            return false;
        }
    } else {
        return false;
    }
    
    let output_path = Path::new(output_log);
    if path == output_path {
        return false;
    }
    
    if let Some(filename) = path.file_name() {
        if let Some(filename_str) = filename.to_str() {
            return filename_str.to_lowercase().contains(filename_filter);
        }
    }
    
    false
}

fn is_gz_file(path: &PathBuf) -> bool {
    if !path.is_file() {
        return false;
    }
    
    if let Some(extension) = path.extension() {
        return extension == "gz";
    }
    
    false
}

fn process_file(
    path: &PathBuf,
    search_terms: &[SearchTerm],
    line_filter: &str,
    output_file: &mut File,
) -> usize {
    if let Some(filename) = path.file_name() {
        if let Some(filename_str) = filename.to_str() {
            print!("{} [", filename_str);
            
            let file = match File::open(path) {
                Ok(file) => file,
                Err(e) => {
                    eprintln!("Error opening file {}: {}", path.display(), e);
                    return 0;
                }
            };
            
            let reader = BufReader::new(file);
            let file_match_count = process_reader(reader, search_terms, line_filter, output_file);
            
            if file_match_count == 0 {
                print!("{}", NO_RESULT);
            } else if file_match_count >= BAR_LENGTH {
                print!("+");
            }
            
            println!("]");
            return file_match_count;
        }
    }
    
    0
}

fn process_gz_file(
    gz_path: &PathBuf,
    search_terms: &[SearchTerm],
    line_filter: &str,
    output_file: &mut File,
) -> Result<usize, io::Error> {
    if let Some(gz_filename) = gz_path.file_name() {
        if let Some(gz_filename_str) = gz_filename.to_str() {
            print!("{} (gzipped) [", gz_filename_str);
            
            let file = File::open(gz_path)?;
            let gz = GzDecoder::new(file);
            let reader = BufReader::new(gz);
            let file_match_count = process_reader(reader, search_terms, line_filter, output_file);
            
            if file_match_count == 0 {
                print!("{}", NO_RESULT);
            } else if file_match_count >= BAR_LENGTH {
                print!("+");
            }
            
            println!("]");
            return Ok(file_match_count);
        }
    }
    
    Ok(0)
}

fn process_reader<R: BufRead>(
    reader: R, 
    search_terms: &[SearchTerm],
    line_filter: &str,
    output_file: &mut File,
) -> usize {
    let mut file_match_count = 0;
    
    for line in reader.lines() {
        if let Ok(line) = line {
            let lowercase_line = line.to_lowercase();
            
            let is_match = search_terms.iter().any(|term| {
                lowercase_line.contains(line_filter) &&
                (term.keyword.is_empty() || lowercase_line.contains(&term.keyword)) &&
                (term.additional_keyword.is_empty() || lowercase_line.contains(&term.additional_keyword))
            });
            
            if is_match {
                file_match_count += 1;
                if file_match_count < BAR_LENGTH {
                    print!("{}", BAR_FILLER);
                }
                
                if let Err(e) = writeln!(output_file, "{}", line) {
                    eprintln!("Error writing to output file: {}", e);
                }
            }
        }
    }
    
    file_match_count
}