use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::io::BufRead;
use std::time;

struct StationData {
    min: f32,
    max: f32,
    sum: f32,
    counts: f32
}


fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        println!("no filename specified");
        return;
    }

    let filename = args[1].clone();

    let start_time = time::Instant::now();
    let file = File::open(&filename).unwrap();
    let line_reader = io::BufReader::with_capacity(1024 * (1024 * 512), file).lines();
    // let data = fs::read_to_string(&filename).unwrap_or_default();
    // if data == String::default() {
    //     println!("no data to be read");
    //     return;
    // }

    let mut station_map: HashMap<String, StationData> = HashMap::with_capacity(50000);
    line_reader.for_each(|s| reading_from_str(s.unwrap(), &mut station_map));

    let end_time = start_time.elapsed();

    for item in station_map {
        println!("{} -> min: {}, max: {}, mean: {}", item.0, item.1.min, item.1.max, item.1.sum / item.1.counts);
    }

    println!("{}", filename);
    // println!("{}", data);
    println!("time elapsed: {}ms", end_time.as_millis());
}

fn reading_from_str<'a>(string: String, station_map: &mut HashMap<String, StationData>) {
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
    // println!("time elapsed: 1 {}ns, 2 {}ns, 3 {}ns, total {}ns", time_1, time_2, time_3, start_time.elapsed().as_nanos());
    return
}
