mod generate;

use core::num;
use std::hash::Hash;
use std::io::{Read, Seek};
use std::collections::HashMap;

use rayon::prelude::*;

struct Record {
    min: i16,
    max: i16,
    total: u64,
    count: u64,
}

fn main() {
    // This file reads in 1 billion rows of data from
    // measurements.txt.
    // The file is a "csv" file, each line being name;temp
    // name is a string and temp is a float with one decimal

    // Stream in the file as bytes, not all at once
    let file = std::fs::File::open("measurements.txt");

    // If the file is not found, generate the file
    if file.is_err() {
        println!("File not found, generating file");
        let _ = generate::main();
        return;
    }

    let file = file.unwrap();

    let size = file.metadata().unwrap().len();

    // Read in the file in chunks
    let mut chunk_start = 0;
    let mut chunk_end = 0;
    // Chunk size is file size divided by number of threads
    let chunk_size = size / rayon::current_num_threads() as u64;

    println!("Chunk size: {}", chunk_size);

    // Create a vector of tuples, each tuple containing the start and end of a chunk
    let chunk_specs = (0..rayon::current_num_threads()).map(|_| {
        chunk_end = chunk_start + chunk_size;

        // The end of the chunk doesn't necessarily end at the end of a line, 
        // so we need to read until we hit a \n character
        // We do this by creating a new reader for each chunk, seeking to the end of the chunk,
        // and reading until we hit a \n character
        let mut reader = std::io::BufReader::new(std::fs::File::open("measurements.txt").unwrap());
        reader.seek(std::io::SeekFrom::Start(chunk_end)).unwrap();
        let mut reader_bytes = reader.bytes();
        let mut offset = 0;

        while let Some(Ok(c)) = reader_bytes.next() {
            offset += 1;
            if c == b'\n' {
                break;
            }
        }

        let chunk_end = chunk_end + offset;

        // Return the start and end of the chunk
        let to_return = (chunk_start, size.min(chunk_end));

        // Before next loop, set the start of the next chunk to the end of the current chunk
        chunk_start = chunk_end;
        
        to_return
    }).collect::<Vec<(u64, u64)>>();

    println!("{:?}", chunk_specs);

    // Parallelize the reading of the file, calling the read_chunk function on each chunk
    let data = chunk_specs.into_par_iter().map(|(start, end)| {
        read_chunk("measurements.txt", start, end)
    }).reduce(HashMap::new, |mut map1, map2| {
        for (key, value) in map2 {
            if map1.contains_key(&key) {
                let mut entry: (i16, i16, u64, u64) = *map1.get_mut(&key).unwrap();
                entry.0 = entry.0.min(value.0);
                entry.1 = entry.1.max(value.1);
                entry.2 += value.2;
                entry.3 += value.3;
            } else {
                map1.insert(key.clone(), value);
            }
        }

        map1
    });


    /*
    The program should print out the min, mean, and max values per station, alphabetically ordered. The format that is expected varies slightly from language to language, but the following example shows the expected output for the first three stations:

    Hamburg;12.0;23.1;34.2
    Bulawayo;8.9;22.1;35.2
    Palembang;38.8;39.9;41.0
     */

    let mut data = data.into_iter().collect::<Vec<_>>();
    data.sort_unstable_by(|a, b| a.0.cmp(&b.0));

    for (key, value) in data {
        let mean = (value.2 as f64 / value.3 as f64) / 10.;
        // 
        let mean = (mean * 10.0).round() / 10.0;

        let min = value.0 as f64 / 10.;
        let max = value.1 as f64 / 10.;

        let min = (min * 10.0).round() / 10.0;
        let max = (max * 10.0).round() / 10.0;
        println!("{};{};{};{}", std::str::from_utf8(&key).unwrap(), min, mean, max);
    }


}


fn read_chunk(file: &str, start: u64, end: u64) -> HashMap<Vec<u8>, Record>{
    let mut reader = std::io::BufReader::new(std::fs::File::open(file).unwrap());
    reader.seek(std::io::SeekFrom::Start(start)).unwrap();
    let mut reader_bytes = reader.bytes();


    // Return a hashmap of the data, with the name as the key and the values of
    // - min
    // - max
    // - total
    // - count
    // All temps are multiplied by 10

    let mut data_map: HashMap<Vec<u8>, Record> = std::collections::HashMap::new();

    let mut bytes_consumed = 0;
    let mut c;
    
    let mut name = Vec::with_capacity(124);
    let mut temp = Vec::with_capacity(8);

    let total_bytes = end - start;

    loop {
        name.clear();
        temp.clear();

        // Read in the name, byte by byte until
        // the semicolon is found. We don't want
        // to include the semicolon in the name so
        // we break the loop when we find it
        loop {
            c = reader_bytes.next().unwrap().unwrap();
            bytes_consumed += 1;

            if c == b';' {
                break;
            } else {
                name.push(c);
            }
        }
        

        // Read in the temperature, byte by byte.
        // It's the same general idea as the name,
        // but we also need to check for a period
        // which we skip
        loop {
            c = reader_bytes.next().unwrap().unwrap();
            bytes_consumed += 1;

            if c == b'\n' {
                break;
            } else if c != b'.' {
                temp.push(c);
            }
        }

        let is_negative = temp[0] == b'-';

        // Convert the temperature to an i16 
        // by converting the bytes to digits
        let mut temp_num = 0;
        let mut num_real_digits = 0;
        for &digit in temp.iter() {
            if digit == b'-' || digit == b'.'{
                continue;
            }

            temp_num += (digit - b'0') as i16 * 10_i16.pow(num_real_digits);
            num_real_digits += 1;
        }

        if is_negative {
            temp_num *= -1;
        }

        if let Some(entry) = data_map.get_mut(&name) {
            
        } else {
            data_map.insert(name.clone(), Record::new(temp_num as i16));
        }

        if bytes_consumed >= total_bytes {
            break;
        }
    }

    println!("Bytes consumed: {}", bytes_consumed);

    data_map
}