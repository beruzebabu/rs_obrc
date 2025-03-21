use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::os::windows::fs::FileExt;
use std::time;

struct StationData {
    min: f32,
    max: f32,
    sum: f32,
    counts: f32
}

const BUFSIZE: usize = 4096 * 32;

fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        println!("no filename specified");
        return;
    }

    let filename = args[1].clone();

    let start_time = time::Instant::now();
    let file = File::open(&filename).unwrap();
    let mut offset = 0;
    let mut station_map: HashMap<String, StationData> = HashMap::with_capacity(50000);
    loop {
        let count = read_file_between_offsets(&file, offset, &mut station_map).unwrap();
        if count == 0 {
            break
        }

        offset = offset + count as u64;
    }

    let end_time = start_time.elapsed();

    for item in station_map {
        println!("{} -> min: {}, max: {}, mean: {}", item.0, item.1.min, item.1.max, item.1.sum / item.1.counts);
    }

    println!("{}", filename);
    println!("time elapsed: {}ms", end_time.as_millis());
}

fn read_file_between_offsets(file: &File, start: u64, station_map: &mut HashMap<String, StationData>) -> Result<usize, Box<dyn Error>> {
    let mut buf = [0x00; BUFSIZE];
    let offset = start;
    let result = file.seek_read(&mut buf, offset)?;
    if result == 0 || result < 4 {
        return Ok(0);
    }
    let mut bslices: Vec<&[u8]> = buf.split(|&b| b == 0x0a).collect();
    let mut sub = bslices.last().unwrap().len();
    if result == sub {
        reading_from_str(String::from_utf8(bslices.last().unwrap().to_vec())?.as_str(), station_map);
        return Ok(result);
    } else if result < sub {
        sub = 0;
    }
    let count = result - sub;
    bslices.pop();
    for slice in bslices {
        reading_from_str(&String::from_utf8_lossy(slice), station_map);
    }
    return Ok(count);
}

fn reading_from_str<'a>(string: &str, station_map: &mut HashMap<String, StationData>) {
    if string.len() < 4 {
        return
    }

    let result = string.split_once(';').expect("invalid value in text file");
    let float: f32 = result.1.trim().parse().expect("invalid value in text file");

    if !station_map.contains_key(result.0) {
        station_map.insert(result.0.to_string(), StationData{
            min: float,
            max: float,
            sum: float,
            counts: 1.0
        });
    } else {
        let temp = station_map.get_mut(result.0).unwrap();
        if temp.min > float {
            temp.min = float;
        }
        if temp.max < float {
            temp.max = float;
        }
        temp.sum += float;
        temp.counts += 1.0;
    }
    return
}
