import pandas as pd
import folium
from folium.plugins import HeatMap
import branca.colormap as cm

# Load the data
data = pd.read_csv('part-00000-b651a3a3-321f-4036-9946-970d4a513025-c000.csv')

# Drop rows with NaN values in 'latitude' or 'longitude' or 'frequency'
data.dropna(subset=['latitude', 'longitude', 'frequency'], inplace=True)

# Prepare the data for the heatmap
heat_data = [[row['latitude'], row['longitude'], row['frequency']] for index, row in data.iterrows()]

# Create a base map
m = folium.Map(location=[0, 0], zoom_start=2, max_bounds=True, control_scale=True, min_zoom=2, max_zoom=10)

# Create a colormap for the legend
colormap = cm.LinearColormap(colors=['blue', 'green', 'yellow', 'orange', 'red'], vmin=min(data['frequency']), vmax=max(data['frequency']))
colormap.caption = 'Server Usage'

# Add the heatmap layer with varying colors
HeatMap(heat_data, max_val=data['frequency'].max(), gradient={0.2: 'blue', 0.4: 'green', 0.6: 'yellow', 0.8: 'orange', 1: 'red'}).add_to(m)

# Add a title and subheading to the map
title_html = '''
    <div style="position: fixed; top: 20px; left: 50%; transform: translateX(-50%); width: auto; height: auto; background-color: white; 
                border-radius: 25px; z-index: 9999; padding: 10px; box-shadow: 0px 0px 15px rgba(0, 0, 0, 0.2);">
        <h3 align="center" style="font-size:20px; margin: 0;"><b>Common Crawl Server Heatmap</b></h3>
        <h4 align="center" style="font-size:16px; margin: 0;">Sam Struthers S1091108</h4>
    </div>
    '''
m.get_root().html.add_child(folium.Element(title_html))

# Add the colormap to the map
colormap.add_to(m)

# Save the map as an HTML file
m.save("heat_map.html")

print("Heatmap has been created and saved as heat_map.html")