use std::collections::{HashMap, VecDeque};
use std::error::Error;
use std::fs::File;
use std::os::windows::fs::FileExt;
use std::sync::mpsc::channel;
use std::thread::{JoinHandle};
use std::{thread, time};

#[derive(Debug)]
struct StationData {
    min: f32,
    max: f32,
    sum: f32,
    counts: f32
}

const BUFSIZE: usize = 4096 * 48;
const MAX_THREADS: usize = 64;

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
    let mut handles: VecDeque<JoinHandle<HashMap<String, StationData>>> = VecDeque::with_capacity(512);
    let mut active_threads: usize = 0;
    loop {
        if active_threads >= MAX_THREADS {
            let handle = handles.pop_front().unwrap();
            let data = handle.join().unwrap();
            for (name, stationdata) in data {
                if !station_map.contains_key(name.as_str()) {
                    station_map.insert(name, StationData{
                        min: stationdata.min,
                        max: stationdata.max,
                        sum: stationdata.sum,
                        counts: 1.0
                    });
                } else {
                    let temp = station_map.get_mut(name.as_str()).unwrap();
                    if temp.min > stationdata.min {
                        temp.min = stationdata.min;
                    }
                    if temp.max < stationdata.max {
                        temp.max = stationdata.max;
                    }
                    temp.sum += stationdata.sum;
                    temp.counts += 1.0;
                }
            }
            active_threads -= 1;
        }
        let mut buf = [0x00; BUFSIZE];
        let result = file.seek_read(&mut buf, offset).unwrap();
        if result < 4 {
            break
        }

        let end_pos = buf.iter().rposition(|&b| b == 0x0a).unwrap();
        let end_pos_t = end_pos.clone();
        let thandle = thread::spawn(move || {
            let mut map: HashMap<String, StationData> = HashMap::with_capacity(500);
            read_buf_between_offsets(&buf[..end_pos_t], &mut map).unwrap();
            return map
        });
        handles.push_back(thandle);
        active_threads += 1;

        offset = offset + end_pos as u64;
    }

    let total_handles = handles.len();
    println!("{}", total_handles);

    for handle in handles {
        let data = handle.join().unwrap();
        for (name, stationdata) in data {
            if !station_map.contains_key(name.as_str()) {
                station_map.insert(name, StationData{
                    min: stationdata.min,
                    max: stationdata.max,
                    sum: stationdata.sum,
                    counts: 1.0
                });
            } else {
                let temp = station_map.get_mut(name.as_str()).unwrap();
                if temp.min > stationdata.min {
                    temp.min = stationdata.min;
                }
                if temp.max < stationdata.max {
                    temp.max = stationdata.max;
                }
                temp.sum += stationdata.sum;
                temp.counts += 1.0;
            }
        }
    }

    let end_time = start_time.elapsed();
    let mut collection: Vec<(&String, &StationData)> = station_map.iter().map(|s| (s.0, s.1)).collect();
    collection.sort_by(|a, b| a.0.cmp(b.0));

    for item in collection {
        println!("{} -> min: {}, max: {}, mean: {}", item.0, item.1.min, item.1.max, item.1.sum / item.1.counts);
    }

    println!("{}", total_handles);
    println!("{}", filename);
    println!("time elapsed: {}ms", end_time.as_millis());
}

fn read_buf_between_offsets(buf: &[u8], station_map: &mut HashMap<String, StationData>) -> Result<usize, Box<dyn Error>> {
    let result = buf.len();
    if result == 0 || result < 4 {
        return Ok(0);
    }
    let mut bslices: Vec<&[u8]> = buf.split(|&b| b == 0x0a).collect();
    for slice in bslices {
        reading_from_str(&String::from_utf8_lossy(slice), station_map);
    }
    return Ok(result);
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
