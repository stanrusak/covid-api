def parse_locations(data, request):

    locations = set()
    if request.country and isinstance(request.country, str):
        locations.add(request.country)
    elif request.country and isinstance(request.country, list):
        locations = locations.union(set(request.country))

    if request.location and isinstance(request.location, str):
        locations.add(request.location)
    elif request.location and isinstance(request.location, list):
        locations = locations.union(set(request.location))
    
    all_locations = data.columns.get_level_values(0).unique()
    diff = locations.difference(set(all_locations))
    if diff:
        return {"Error": f"Locations {diff} are not in the data. Check `/locations` for a list of valid locations."}

    return sorted(list(locations)) if locations else all_locations

def parse_period(request):

    start, end = request.start, request.end
    if request.period:
        start, end = request.period.split(':')
    if request.start:
        start = request.start
    if request.end:
        end = request.end
    
    return start, end

def validate_columns(data, request):

    columns = data.columns.get_level_values(1).unique().to_list()
    
    if request.columns:
        
        cols = []
        for col in request.columns:
            
            if col not in columns:
                return {"Error": f"Column name `{col}` is invalid. Check `/columns` for a list of valid columns"}
            cols.append(col)
        columns = cols
    
    return columns

def validate_locations(data, locations):

    valid_locations = data.columns.get_level_values(0).unique().to_list()

    result = []
    if isinstance(locations, str):
        locations = [locations]
    
    for location in locations:

        if location in valid_locations:
            result.append(location)
        else:
            return {"Error": f"Invalid location {location}"}
    
    return sorted(result)


def get_location_data(location, data, columns, start, end):

    location_data = data.loc[slice(start,end)].xs(location, axis=1, level=0)
    location_data = location_data[columns].fillna('null')

    d = {'date': location_data.index.to_list()}
    for column in columns:
        d[column] = location_data[column].to_list()

    return d