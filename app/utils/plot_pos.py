from pathlib import Path
import os
import pandas as pd
from datetime import datetime
import plotly.graph_objects as go
from statsmodels.nonparametric.smoothers_lowess import lowess
import numpy as np
import argparse

def parse_pos_format(file_path):
    """
    Parse a .POS file into a pandas DataFrame.

    Arguments:
      file_path (str): Path to a .POS file with records following the expected format.

    Returns:
      pandas.DataFrame: Table with columns such as 'Time', 'Latitude', 'Longitude',
      'Elevation', 'dN', 'dE', 'dU', 'sN', 'sE', 'sU', 'sElevation', 'Rne', 'Rnu', 'Reu', 'soln'.

    Example (Optional):
      >>> df = parse_pos_format("sample.POS")  # doctest: +SKIP
      >>> isinstance(df, pd.DataFrame)  # doctest: +SKIP
      True
    """
    data = []
    try:
        with open(file_path, 'r') as file:
            data_started = False
            for line in file:
                if line.startswith('*'):
                    data_started = True
                    continue
                if data_started:
                    parts = line.split()
                    if len(parts) >= 24:
                        record = {
                            'Time': datetime.strptime(parts[0], '%Y-%m-%dT%H:%M:%S.%f'),
                            'Latitude': float(parts[11]),
                            'Longitude': float(parts[12]),
                            'Elevation': float(parts[13]),
                            'dN': float(parts[14]),
                            'dE': float(parts[15]),
                            'dU': float(parts[16]),
                            'sN': float(parts[17]),
                            'sE': float(parts[18]),
                            'sU': float(parts[19]),
                            'sElevation': float(parts[19]),
                            'Rne': float(parts[20]),
                            'Rnu': float(parts[21]),
                            'Reu': float(parts[22]),
                            'soln': parts[23]
                        }
                        data.append(record)
    except Exception as e:
        print(f"Error parsing file {file_path}: {e}")                
    return pd.DataFrame(data)

# Function to parse the datetime with optional timezone
def parse_datetime(datetime_str):
    """
    Parse an ISO-like datetime string with or without timezone into a naive datetime.

    Arguments:
      datetime_str (str): Datetime string, e.g., 'YYYY-MM-DDTHH:MM:SS' or 'YYYY-MM-DDTHH:MM:SS±HHMM'.

    Returns:
      datetime: Timezone-naive datetime object. If timezone was present, it is stripped.

    Example (Optional):
      >>> parse_datetime("2025-01-01T00:00:00")  # doctest: +SKIP
    """
    # Attempt to parse datetime with and without timezone
    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S"):
        try:
            parsed_datetime = datetime.strptime(datetime_str, fmt)
            # Make datetime timezone-naive if it includes timezone information
            if parsed_datetime.tzinfo is not None:
                parsed_datetime = parsed_datetime.replace(tzinfo=None)
            return parsed_datetime
        except ValueError as e:
            print(f"ValueError: {e}")
            continue
    raise ValueError(f"datetime {datetime_str} does not match expected formats.")    

def remove_weighted_mean(data):
    """
    Remove weighted mean from each component series in-place and return the DataFrame.

    Arguments:
      data (pandas.DataFrame): Data with components ('dN','dE','dU','Elevation') and their sigmas ('sN','sE','sU','sElevation').

    Returns:
      pandas.DataFrame: The same DataFrame with each component demeaned by its weighted mean.
    """
    sigma_keys = {'dN': 'sN', 'dE': 'sE', 'dU': 'sU', 'Elevation': 'sElevation'}  # Assume sElevation exists
    for component in ['dN', 'dE', 'dU', 'Elevation']:
        sigma_key = sigma_keys[component]
        weights = 1 / data[sigma_key]**2
        weighted_mean = np.average(data[component], weights=weights)
        data[component] -= weighted_mean  # Demean the series
    return data

def apply_smoothing(data, horz_smoothing=None, vert_smoothing=None):
    """
    Apply LOWESS smoothing to horizontal and/or vertical components.

    Arguments:
      data (pandas.DataFrame): Input data with time column 'Time' and components.
      horz_smoothing (float or None): Fraction for LOWESS on dN/dE (0..1), or None to skip.
      vert_smoothing (float or None): Fraction for LOWESS on dU/Elevation (0..1), or None to skip.

    Returns:
      pandas.DataFrame: DataFrame with additional 'Smoothed_*' columns when smoothing is applied.

    Example (Optional):
      >>> apply_smoothing(df, horz_smoothing=0.1)  # doctest: +SKIP
    """
    for component in ['dN', 'dE', 'dU', 'Elevation']:
        if horz_smoothing and (component == 'dN' or component == 'dE'):
            data[f'Smoothed_{component}'] = lowess(data[component], data['Time'], frac=horz_smoothing, return_sorted=False)
        if vert_smoothing and (component == 'dU' or component == 'Elevation'):
            data[f'Smoothed_{component}'] = lowess(data[component], data['Time'], frac=vert_smoothing, return_sorted=False)
    return data

def compute_statistics(data):
    """
    Compute weighted statistics and add helper series for plotting.

    Arguments:
      data (pandas.DataFrame): Data containing components and corresponding sigma columns.

    Returns:
      tuple[pandas.DataFrame, dict]: (data_with_helper_columns, stats_dict) where stats_dict maps
      component -> {'weighted_mean', 'std_dev', 'rms'}.

    Example (Optional):
      >>> data, stats = compute_statistics(df)  # doctest: +SKIP
    """
    stats = {}
    for component in ['dN', 'dE', 'dU', 'Elevation']:
        sigma_key = f's{component[-1].upper()}' if component != 'Elevation' else 'sElevation'
        weights = 1 / data[sigma_key]**2

        weighted_mean = np.average(data[component], weights=weights)
        std_dev = np.sqrt(np.average((data[component] - weighted_mean)**2, weights=weights))
        rms = np.sqrt(np.mean(data[component]**2))

        # Store calculated statistics
        stats[component] = {
            'weighted_mean': weighted_mean,
            'std_dev': std_dev,
            'rms': rms
        }

        # Prepare series for plotting
        data[f'{component}_weighted_mean'] = [weighted_mean] * len(data)
        data[f'{component}_std_dev_upper'] = weighted_mean + std_dev*2.0
        data[f'{component}_std_dev_lower'] = weighted_mean - std_dev*2.0

    return data, stats

def create_plots(all_data, input_files, component_stats, args):
    """
    Create interactive HTML plots for POS analysis (time-series, dN–dE scatter, and optional map/heatmap).

    Arguments:
      all_data (pandas.DataFrame): Parsed measurement table with columns such as 'Time', 'dN', 'dE', 'dU' or 'Elevation', and their sigmas.
      input_files (list[str]): One or more .POS file paths used for titles and output naming.
      component_stats (dict): Per-component statistics returned by compute_statistics().
      args (object): Options namespace, supporting fields like colour_sigma, max_sigma, elevation, map, heatmap, save_prefix.

    Returns:
      None: Writes HTML files when args.save_prefix is provided; otherwise keeps figures in memory.

    Example (Optional):
      >>> # create_plots(df, ["site.POS"], stats, SimpleNamespace(colour_sigma=False, save_prefix="./out/fig"))  # doctest: +SKIP
    """
    input_root = Path(input_files[0]).stem
    
    # Start plotting
    # Determine max sigma and color scale settings for Fig1
    title_text = f"<b>Time Series Analysis</b>: {', '.join(input_files)}<br>"
    color_scale = 'Jet' if args.colour_sigma else None  # Only set color scale if --colour_sigma is active
    max_sigma_data = np.max([all_data['sN'].max(), all_data['sE'].max(), all_data['sU'].max()])
    min_sigma_data = np.min([all_data['sN'].min(), all_data['sE'].min(), all_data['sU'].min()])
    cmax = min(args.max_sigma, max_sigma_data) if args.max_sigma is not None else max_sigma_data
    #cmin = min_sigma_data 
    cmin = 0.0 

    # Setting up the plot
    fig1 = go.Figure()
    components = ['dN', 'dE', 'Elevation'] if args.elevation else ['dN', 'dE', 'dU']
    component_colors = {
        'dN': 'red',
        'dE': 'green',
        'dU': 'blue',
        'Elevation': 'orange'
    }

    for component in components:
        # Correctly map the component to its sigma key
        if component == 'Elevation':
            sigma_key = 'sU'  # Assuming sigma for Elevation is stored in 'sU'
        else:
            sigma_key = f's{component[-1].upper()}'
        
        print('Plotting: ', sigma_key)  # To check if the correct sigma key is being used

        # Add the primary and smoothed series data
        if args.colour_sigma:
            # When using --colour_sigma, use the sigma value for coloring
            fig1.add_trace(go.Scatter(
                x=all_data['Time'], y=all_data[component],
                mode='lines+markers',
                marker=dict(size=5, color=all_data[sigma_key], coloraxis="coloraxis"),
                name=component,
                hoverinfo='text+x+y',
                text=f'{component} Sigma: ' + all_data[sigma_key].astype(str)
            ))

        else:
            # When not using --colour_sigma, add error bars using the sigma values
            color = component_colors[component]
            fig1.add_trace(go.Scatter(
                x=all_data['Time'], y=all_data[component],
                mode='markers',
                name=component,
                error_y=dict(
                    type='data',  # Represent error in data coordinates
                    array=all_data[sigma_key],  # Positive error
                    arrayminus=all_data[sigma_key],  # Negative error
                    visible=True,  # Make error bars visible
                    color='gray'  # Color of error bars
                ),
                marker=dict(size=5, color=color),
                line=dict(color=color),
                hoverinfo='text+x+y',
                text=f'{component} Sigma: ' + all_data[sigma_key].astype(str)
            ))

        if f'Smoothed_{component}' in all_data:
            fig1.add_trace(go.Scatter(
                x=all_data['Time'], y=all_data[f'Smoothed_{component}'],
                mode='lines',
                name=f'Smoothed {component}',
                line=dict(color='rgba(0,0,255,0.5)')
            ))

        # Add statistical lines and shaded areas for standard deviation
        fig1.add_trace(go.Scatter(
            x=all_data['Time'], y=all_data[f'{component}_weighted_mean'],
            mode='lines',
            name=f'{component} Weighted Mean',
            line=dict(color='red')
        ))

        fig1.add_trace(go.Scatter(
            x=all_data['Time'].tolist() + all_data['Time'].tolist()[::-1],
            y=all_data[f'{component}_std_dev_upper'].tolist() + all_data[f'{component}_std_dev_lower'].tolist()[::-1],
            fill='toself',
            fillcolor='rgba(68, 68, 255, 0.2)',
            line=dict(color='rgba(255,255,255,0)'),
            name=f'{component} CI: 2 Sigma (95%)'
        ))

        stats = component_stats[component]
        title_text += f"<b>{component}</b>: Weighted Mean = {stats['weighted_mean']:.3f}, Std Dev = {stats['std_dev']:.3f}, RMS = {stats['rms']:.3f}<br>"

    fig1.update_layout(
        title=title_text,
        xaxis_title='Time',
        yaxis_title='Measurement Value',
        xaxis=dict(
            rangeslider=dict(visible=True),
            fixedrange=False,  
            type='date'
        ),
        yaxis=dict(
            fixedrange=False
        ),
        coloraxis=dict(
            colorscale=color_scale,
            cmin=cmin,
            cmax=cmax,
            colorbar=dict(
                title='Sigma Value',
                x=0.5,  # Center the color bar on the x-axis
                y=-0.5,  # Position the color bar below the x-axis
                xanchor='center',  # Anchor the color bar at its center for x positioning
                yanchor='bottom',  # Anchor the color bar from its bottom edge for y positioning
                len=0.5,  # Length of the color bar (75% of the width of the plot area)
                thickness=10,  # Thickness of the color bar
                orientation='h'  # Horizontal orientation
            ),
        ) if args.colour_sigma else {},
        showlegend=True,
        margin=dict(t=150) 
    )
    
    if args.save_prefix is not None:
        output_path = os.path.join(os.path.dirname(args.save_prefix), f"{input_root}_fig1.html")
        os.makedirs(os.path.dirname(output_path) or '.', exist_ok=True)
        fig1.write_html(output_path)
    
    title_text = f"<b>dN vs dE Analysis</b>: {', '.join(input_files)}<br>"
    for component in ['dN', 'dE']:
        stats = component_stats[component]
        title_text += f"<b>{component}</b>: Weighted Mean = {stats['weighted_mean']:.3f}, Std Dev = {stats['std_dev']:.3f}, RMS = {stats['rms']:.3f}<br>"

    composite_uncertainty = np.sqrt(all_data['sN']**2 + all_data['sE']**2)
    all_data['composite_uncertainty'] = composite_uncertainty
    max_sigma_data = composite_uncertainty.max()
    if args.colour_sigma:
        cmax = min(args.max_sigma, max_sigma_data) if args.max_sigma is not None else max_sigma_data
        cmin = composite_uncertainty.min()
        #cmin = 0.0
        color_scale = 'Jet'  # Define the color scale here within the condition
    else:
        cmin = None  # No cmax needed for static colors
        cmax = None  # No cmax needed for static colors
        color_scale = None  # No color scale needed for static colors

    # Plot configuration
    fig2 = go.Figure()
    fig2.add_trace(go.Scatter(
        x=all_data['dE'], y=all_data['dN'],
        mode='markers',
        marker=dict(
            size=5,
            color=all_data['composite_uncertainty'] if args.colour_sigma else 'blue',  
            coloraxis="coloraxis" if args.colour_sigma else None  
        ),
        name='dE vs dN',
        text=[f"{time} Sigma dNdE: {unc:.4f}" for time, unc in zip(all_data['Time'], all_data['composite_uncertainty'])],
        hoverinfo='text+x+y'
    ))

    if 'Smoothed_dN' in all_data.columns and 'Smoothed_dE' in all_data.columns:
        fig2.add_trace(go.Scatter(
            x=all_data['Smoothed_dE'], y=all_data['Smoothed_dN'],
            mode='markers',
            marker=dict(
                size=5,
                color='red'
            ),
            name='Smoothed'
        ))

    fig2.update_layout(
        title=title_text,
        xaxis_title='dE (meters)',
        yaxis_title='dN (meters)',
        xaxis=dict(scaleanchor="y", scaleratio=1),
        yaxis=dict(scaleanchor="x", scaleratio=1),
        coloraxis=dict(
            colorscale=color_scale,
            cmin=cmin,
            cmax=cmax,
            colorbar=dict(
                title='Sigma Value',
                x=0.5, y=-0.15,  # Adjusted for visibility
                xanchor='center', yanchor='bottom',
                len=0.75, thickness=20, orientation='h'
            )
        ) if args.colour_sigma else None,  # Apply color axis settings only if needed
        showlegend=True
    )
    
    if args.save_prefix is not None:
        output_path = os.path.join(os.path.dirname(args.save_prefix), f"{input_root}_fig2.html")
        os.makedirs(os.path.dirname(output_path) or '.', exist_ok=True)
        fig2.write_html(output_path)
    
    if args.map:
        def adjust_zoom(latitudes, longitudes):
            lat_range = np.ptp(latitudes)  # Peak to peak (range) of latitudes
            lon_range = np.ptp(longitudes)  # Peak to peak (range) of longitudes
            if max(lat_range, lon_range) < 0.02:
                return 13  # City level zoom
            elif max(lat_range, lon_range) < 0.1:
                return 10  # Regional level zoom
            elif max(lat_range, lon_range) < 1:
                return 7  # Country level zoom
            else:
                return 5  # Continental level zoom

        zoom_level = adjust_zoom(all_data['Latitude'], all_data['Longitude'])

        fig3 = go.Figure(go.Scattermapbox(
            lat=all_data['Latitude'],
            lon=all_data['Longitude'],
            mode='markers+lines',
            marker=dict(size=5, color='blue')
        ))

        fig3.update_layout(
            mapbox=dict(
                style="open-street-map",
                center=go.layout.mapbox.Center(
                    lat=all_data['Latitude'].mean(), 
                    lon=all_data['Longitude'].mean()
                ),
                zoom=zoom_level
            ),
            title='Geographic Plot of Latitude and Longitude',
            showlegend=False
        )

        # Save fig3 if requested
        if args.save_prefix is not None:
            output_path = os.path.join(os.path.dirname(args.save_prefix), f"{input_root}_fig3.html")
            os.makedirs(os.path.dirname(output_path) or '.', exist_ok=True)
            fig3.write_html(output_path)
    
    if args.heatmap:
        # Plotly plotting dN vs dE heatmap  (not used)
        fig4 = go.Figure()
        fig4.add_trace(go.Histogram2dContour(
            x = all_data['dE'],
            y = all_data['dN'],
            colorscale = 'Jet',
            reversescale = False,
            xaxis = 'x',
            yaxis = 'y'
        ))
        fig4.add_trace(go.Scatter(
            x = all_data['dE'],
            y = all_data['dN'],
            xaxis = 'x',
            yaxis = 'y',
            mode = 'markers',
            marker = dict(
                color = 'rgba(0,0,0,0.3)',
                size = 3
            )
        ))
        fig4.add_trace(go.Scatter(
            x = all_data['dE_weighted_mean'],
            y = all_data['dN_weighted_mean'],
            xaxis = 'x',
            yaxis = 'y',
            mode = 'markers',
            marker = dict(
                color="white",
                size = 15,
                line_color='black',
                symbol='x-dot',
                line_width=2
            ),
            hoverinfo='text+x+y',
            text='Weighted Mean (dE, dN)'
        ))
        fig4.add_trace(go.Histogram(
            y = all_data['dN'],
            xaxis = 'x2',
            marker = dict(
                color = 'rgba(0,0,0,1)'
            )
        ))
        fig4.add_trace(go.Histogram(
            x = all_data['dE'],
            yaxis = 'y2',
            marker = dict(
                color = 'rgba(0,0,0,1)'
            )
        ))

        fig4.update_layout(
        autosize = False,
        xaxis = dict(
            zeroline = False,
            domain = [0,0.85],
            showgrid = False
        ),
        yaxis = dict(
            zeroline = False,
            domain = [0,0.85],
            showgrid = False
        ),
        xaxis2 = dict(
            zeroline = False,
            domain = [0.85,1],
            showgrid = False
        ),
        yaxis2 = dict(
            zeroline = False,
            domain = [0.85,1],
            showgrid = False
        ),
        title=title_text,
        xaxis_title='dE (meters)',
        yaxis_title='dN (meters)',
        height = 800,
        width = 800,
        bargap = 0,
        hovermode = 'closest',
        showlegend = False
        )

        if args.save_prefix is not None:
            output_path = os.path.join(os.path.dirname(args.save_prefix), f"{input_root}_fig4.html")
            os.makedirs(os.path.dirname(output_path) or '.', exist_ok=True)
            fig4.write_html(output_path)

def run_plot_pos(file_path, output_html="pos_plot.html"):
    """
    Legacy wrapper kept for backward compatibility with older call sites.

    Arguments:
      file_path (str): Path to a single .POS file.
      output_html (str): Target HTML file path (placeholder; current implementation returns it unchanged).

    Returns:
      str: The output_html string passed in.

    Example (Optional):
      >>> run_plot_pos("sample.POS", "pos_plot.html")
      'pos_plot.html'
    """
    df = parse_pos_format(file_path)
    # This function needs to be updated to work with the new structure
    # For now, just return the output path
    return output_html

def plot_pos_files(input_files, start_datetime=None, end_datetime=None, 
                   horz_smoothing=None, vert_smoothing=None, colour_sigma=False,
                   max_sigma=None, elevation=False, demean=False, map_view=False,
                   heatmap=False, sigma_threshold=None, down_sample=None, 
                   save_prefix=None):
    """
       Generate interactive plots from one or more POS files (time window, smoothing, filtering, and outputs).

       Arguments:
         input_files (list[str]): One or more input .POS file paths.
         start_datetime (str or None): Start time (e.g., 'YYYY-MM-DDTHH:MM:SS' with optional timezone).
         end_datetime (str or None): End time (same format as start_datetime).
         horz_smoothing (float or None): LOWESS fraction for horizontal components (dN/dE).
         vert_smoothing (float or None): LOWESS fraction for vertical component (dU or Elevation).
         colour_sigma (bool): If True, color markers by sigma; otherwise show error bars.
         max_sigma (float or None): Upper cap for sigma color scale when colour_sigma is True.
         elevation (bool): If True, use 'Elevation' instead of 'dU' for vertical plotting.
         demean (bool): If True, remove weighted mean from each series before plotting.
         map_view (bool): If True, generate a geographic map (fig3).
         heatmap (bool): If True, generate a 2D dE–dN density view (fig4).
         sigma_threshold (tuple[float, float, float] or None): (sE, sN, sU) thresholds for filtering rows.
         down_sample (int or None): Resampling interval in seconds for time-indexed thinning.
         save_prefix (str or None): If provided, write HTML files next to this prefix.

       Returns:
         list[str]: Paths to generated HTML files when save_prefix is provided; empty list if save_prefix is None.

       Example (Optional):
         >>> files = ["./A.POS", "./B.POS"]  # doctest: +SKIP
         >>> out = plot_pos_files(files, start_datetime="2025-01-01T00:00:00", colour_sigma=False, save_prefix="./out/fig")  # doctest: +SKIP
         >>> isinstance(out, list)  # doctest: +SKIP
         True
       """
    class Args:
        def __init__(self):
            self.start_datetime = start_datetime
            self.end_datetime = end_datetime
            self.horz_smoothing = horz_smoothing
            self.vert_smoothing = vert_smoothing
            self.colour_sigma = colour_sigma
            self.max_sigma = max_sigma
            self.elevation = elevation
            self.demean = demean
            self.map = map_view
            self.heatmap = heatmap
            self.sigma_threshold = sigma_threshold
            self.down_sample = down_sample
            self.save_prefix = save_prefix
    
    args = Args()
    
    start_dt = parse_datetime(start_datetime) if start_datetime else None
    end_dt = parse_datetime(end_datetime) if end_datetime else None

    all_data = pd.DataFrame()
    for file_path in input_files:
        file_data = parse_pos_format(file_path)
        all_data = pd.concat([all_data, file_data], ignore_index=True)

    all_data['Time'] = pd.to_datetime(all_data['Time'], format="%Y-%m-%dT%H:%M:%S.%f")

    if start_dt:
        all_data = all_data[all_data['Time'] >= start_dt]
    if end_dt:
        all_data = all_data[all_data['Time'] <= end_dt]

    if sigma_threshold:
        se_threshold, sn_threshold, su_threshold = sigma_threshold
        mask = (all_data['sE'] <= se_threshold) & (all_data['sN'] <= sn_threshold) & (all_data['sU'] <= su_threshold) & (all_data['sElevation'] <= su_threshold)
        all_data = all_data[mask]    

    if down_sample:
        all_data['Time'] = pd.to_datetime(all_data['Time'])
        all_data.set_index('Time', inplace=True)
        all_data = all_data.resample(f'{down_sample}s').first().dropna().reset_index()

    if demean:
        all_data = remove_weighted_mean(all_data)
    all_data = apply_smoothing(all_data, horz_smoothing, vert_smoothing)
    all_data, component_stats = compute_statistics(all_data)

    create_plots(all_data, input_files, component_stats, args)
    
    if save_prefix:
        input_root = Path(input_files[0]).stem
        generated_files = []
        generated_files.append(os.path.join(os.path.dirname(save_prefix), f"{input_root}_fig1.html"))
        generated_files.append(os.path.join(os.path.dirname(save_prefix), f"{input_root}_fig2.html"))
        if map_view:
            generated_files.append(os.path.join(os.path.dirname(save_prefix), f"{input_root}_fig3.html"))
        if heatmap:
            generated_files.append(os.path.join(os.path.dirname(save_prefix), f"{input_root}_fig4.html"))
        return generated_files
    
    return []

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Plot positional data with optional smoothing and color coding.")
    parser.add_argument('--input-files', nargs='+', required=True, help='One or more input .POS files')
    parser.add_argument('--start-datetime', type=str, 
                        help="Start datetime in the format YYYY-MM-DDTHH:MM:SS, optional timezone")
    parser.add_argument('--end-datetime', type=str, 
                        help="End datetime in the format YYYY-MM-DDTHH:MM:SS, optional timezone")
    parser.add_argument('--horz_smoothing', type=float, default=None,
                        help='Fraction of the data used for horizontal (East and North) LOWESS smoothing (optional).')
    parser.add_argument('--vert_smoothing', type=float, default=None,
                        help='Fraction of the data used for vertical (Up) LOWESS smoothing (optional).')
    parser.add_argument('--colour_sigma', action='store_true',
                        help='Colourize the timeseries using the standard deviation (sigma) values (optional).')
    parser.add_argument('--max_sigma', type=float, default=None,
                        help='Set a maximum sigma threshold for the sigma colour scale (optional).')
    parser.add_argument('--elevation', action='store_true',
                        help='Plot Elevation values inplace of dU wrt the reference coord (optional).')
    parser.add_argument('--demean', action='store_true',
                        help='Remove the mean values from all time series before plotting (optional).')
    parser.add_argument('--map', action='store_true',
                        help='Create a geographic map view from the Longitude & Latitude estiamtes (optional).')
    parser.add_argument('--heatmap', action='store_true',
                        help='Create a 2D heatmap view of E & N coodrinates wrt the reference position  (optional).')
    parser.add_argument('--sigma_threshold', nargs=3, type=float, 
                        help="Thresholds for sE, sN, and sU to filter data.")
    parser.add_argument('--down_sample', type=int, 
                        help="Interval in seconds for down-sampling data.")
    parser.add_argument('--save-prefix', nargs='?', const='plot', default=None,
                        help='Prefix for saving HTML figures, e.g., ./output/fig')
    
    args = parser.parse_args()
    
    plot_pos_files(
        input_files=args.input_files,   
        start_datetime=args.start_datetime,
        end_datetime=args.end_datetime,
        horz_smoothing=args.horz_smoothing,
        vert_smoothing=args.vert_smoothing,
        colour_sigma=args.colour_sigma,
        max_sigma=args.max_sigma,
        elevation=args.elevation,
        demean=args.demean,
        map_view=args.map,
        heatmap=args.heatmap,
        sigma_threshold=args.sigma_threshold,
        down_sample=args.down_sample,
        save_prefix=args.save_prefix
    )