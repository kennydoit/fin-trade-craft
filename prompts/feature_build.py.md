## Parameters
- Target Program: features\feature_build.py
- Input: features\config.yaml
- Database: fin_trad_craft
  - Input Schema: extracted
  - Output Schema: transformed

## Instructions
### pre-process config
1. Read in the config.yaml file
2. The universe variable can have either a universe name or universe id. 
   - Option 1: If universe is formatted as a uuid, then validate it against 
     transformed.symbol_universes
   - Option 2: If universe is not a uuid, then it is assumed to be universe_name
     from transformed.symbol_universes. If option 2, then query transformed.symbol_universes
     and retrieve the universe_id that matches universe_name. If there are multiple distinct universe_id, then the default is to take the latest instance as determined by load_date_time
3. create an output folder in features with {universe_id_timestamp}
4. place a copy of config.yaml in output folder

# STOP: Await further instructions