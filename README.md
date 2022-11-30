# covid-api

API serving COVID-19 data collated by the good folks at [Our World In Data](https://ourworldindata.org/coronavirus). Live version can be found at

https://covid-api.up.railway.app/

## Endpoints

Below is a brief documentation of the endpoints. An interactive documentation can be accessed at the `/docs`

### Root endpoint `/`

- `GET`: Info and metadata
- `POST`: Detailed data query with a JSON post request. Accepted parameters:

  - `location` - Location as a string or a list of locations. If not provided, all locations are returned.
  - `columns` - A list of data columns. See the /columns endpoint for available columns. If not provided, all columns are returned.
  - `start` - Starting date for the data in the format YYYY-MM-DD. If not provided, earliest point in the data is used.
  - `end` - Ending date for the data in the format YYYY-MM-MM-MM. If not provided, latest point in the data is used.

## Other endpoints

All other endpoings are accessed with a `GET` request.

- `/docs`: interactive documentation
- `/columns`: a list of all data columns
- `/locations`: a list of´all locations
- `/locations/<location>`: data for a specific country/location
- `/iso`: a list of country ISO codes
- `/latest`: the latest data for cases and deaths
