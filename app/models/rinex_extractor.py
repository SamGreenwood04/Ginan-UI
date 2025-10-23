from datetime import datetime

class RinexExtractor:
    def __init__(self, rinex_path: str):
        self.rinex_path = rinex_path

    def load_rinex_file(self, rinex_path: str):
        self.rinex_path = rinex_path

    def extract_rinex_data(self, rinex_path: str):
        """
        Opens a .RNX file and extracts the corresponding YAML config information

        :param rinex_path: File path for .RNX file to extract from e.g. "resources/input/ALIC.rnx"
        :raises FileNotFoundError if .RNX file is not found
        """
        system_mapping = {
            "G": "GPS",
            "E": "GAL",
            "R": "GLO",
            "C": "BDS",
            "J": "QZS"
        }
        found_constellations = set()

        def format_time(year, month, day, hour, minute, second):
            """
            Helper function to format the parameters into a usable time string for RNX extraction

            :param year: The year
            :param month: The month
            :param day: The day
            :param hour: The hour
            :param minute: The minute
            :param second: The second
            :returns: Formatting string in format: "[YEAR]-[MONTH]-[DAY]_[HOUR]:[MIN}:[SEC]"
                      i.e. "2000-10-27_16:57:49"
            """
            return f"{year:04d}-{month:02d}-{day:02d}_{hour:02d}:{minute:02d}:{(int(second)):02d}"

        # Iterate through each line of the .RNX file
        previous_observation_dt = None
        epoch_interval = None
        in_header = True
        with open(rinex_path, "r") as file:
            for line in file:
                if in_header: # Within the header section of the .RNX file
                    label = line[60:].strip()

                    if label == "SYS / # / OBS TYPES":
                        system_id = line[0]
                        if system_id in system_mapping:
                            found_constellations.add(system_mapping[system_id])

                    # Read and assign variables if they are present in .RNX file's header
                    # If not present, find them in the observations section
                    if label == "TIME OF FIRST OBS":
                        year = int(line[0:6])
                        month = int(line[6:12])
                        day = int(line[12:18])
                        hour = int(line[18:24])
                        minute = int(line[24:30])
                        second = float(line[30:43])
                        start_epoch = format_time(year, month, day, hour, minute, second)

                    elif label == "TIME OF LAST OBS":
                        year = int(line[0:6])
                        month = int(line[6:12])
                        day = int(line[12:18])
                        hour = int(line[18:24])
                        minute = int(line[24:30])
                        second = float(line[30:43])
                        end_epoch = format_time(year, month, day, hour, minute, second)

                    elif label == "INTERVAL":
                        epoch_interval = int(float(line[0:10])) # float cast converts it into a number, int cast converts to integer

                    elif label == "MARKER NAME":
                        marker_name = line[0:60].strip()

                    elif label == "REC # / TYPE / VERS":
                        receiver_type = line[20:40].strip()

                    elif label == "ANT # / TYPE":
                        antenna_type = line[20:40].strip()
                        second_half = line[40:60].strip()
                        if second_half:
                            antenna_type += f" {second_half}"

                    elif label == "ANTENNA: DELTA H/E/N":
                        h = float(line[0:14].strip())
                        e = float(line[14:28].strip())
                        n = float(line[28:42].strip())
                        antenna_offset = [e, n, h]

                    elif label == "END OF HEADER":
                        in_header = False

                else: # Within the observations section of the .RNX file
                    # Find the start of the next observation
                    if (line.startswith('>')):
                        parts = line[1:].split() # Remove the '>'

                        # Map the observation time values to usable variables
                        year, month, day, hour, minute = map(int, parts[:5])
                        second = float(parts[5])

                        # Calculate the epoch_interval
                        # Is only done once. When it is set, skip this
                        if previous_observation_dt and epoch_interval is None:
                            time1 = datetime(*previous_observation_dt)
                            time2 = datetime(year, month, day, hour, minute, int(second))
                            epoch_interval = int((time2 - time1).total_seconds())

                        # Every iteration overwrites end_epoch, so the last iteration will be the last observation
                        end_epoch = format_time(year, month, day, hour, minute, second)
                        previous_observation_dt = (year, month, day, hour, minute, int(second))

        # Build the list of constellations
        constellations = ", ".join(sorted(found_constellations))

        # Safety checks
        if start_epoch is None:
            raise ValueError("TIME OF FIRST OBS not found in provided .RNX file")
        if end_epoch is None:
            raise ValueError("Could not find last observation time (not in header or observations")
        if epoch_interval is None:
            raise ValueError("Could not determine epoch interval time (in header or observations)")

        return {
            "start_epoch": start_epoch,
            "end_epoch": end_epoch,
            "epoch_interval": epoch_interval,
            "marker_name": marker_name,
            "receiver_type": receiver_type,
            "antenna_type": antenna_type,
            "antenna_offset": antenna_offset,
            "constellations": constellations
        }