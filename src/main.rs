use clap::Parser;
use input_args::Commands;

mod column;
mod err;
mod generate_csv;
mod input_args;
mod util;

fn main() -> Result<(), err::TypeGenErrors> {
    let args = Commands::parse();

    let out_filename = args.get_output_filename();
    if out_filename.exists() && !args.force {
        // file already exists. don't overwrite it
        return Err(err::TypeGenErrors::IO(
            std::io::ErrorKind::AlreadyExists.into(),
        ));
    }

    let out_file = std::fs::File::create(&out_filename)?;
    let mut buf = std::io::BufWriter::new(out_file);

    generate_csv::CsvFileInfo::new(args)
        .analyze_input()?
        .load_data_def()
        .generate(&mut buf)?;

    println!("Generated {out_filename:?}");

    Ok(())
}
