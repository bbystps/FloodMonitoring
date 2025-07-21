<script>
// Define chart and lastId as global variables
var chart;
var lastId = 0; // Keep track of the last fetched ID

// Function to fetch the latest data from the server
function fetchLatestData() {
  console.log("Fetching latest data...");
  // const activeSiteLocation = document.querySelector('.site-location.active');
  // const activeSensor = activeSiteLocation.getAttribute('data-region');
  // const url = `fetch_history.php?sensor_id=${activeSensor}&last_id=${lastId}`;
  
  // let station = $('#station_name').text();
  let station = $('#station_name').text().toLowerCase().replace(/ /g, '_');
  console.log(station);
  const url = `fetch_history.php?station=${station}&last_id=${lastId}`;

  fetch(url)
    .then(response => response.json())
    .then(newData => {
      console.log('Fetched new data:', newData);
      if (newData.length > 0) {
        // Update lastId with the latest ID from the new data
        lastId = Math.max(...newData.map(d => d.id));
        console.log('Updated lastId:', lastId);

        // Append new data to the chart's existing data
        newData.forEach(dataPoint => {
          console.log('Adding dataPoint:', dataPoint);
          chart.data.push(dataPoint);
        });

        // Notify the chart about the new data
        chart.invalidateData();
        console.log('Chart updated with new data.');
      } else {
        console.log('No new data to add.');
      }
    })
    .catch(error => console.error('Error fetching latest data:', error));
}

// Function to initialize or update the chart
function updateChart() {
  // const activeSiteLocation = document.querySelector('.site-location.active');
  // const activeSensor = activeSiteLocation.getAttribute('data-region');
  // const url = `fetch_history.php?sensor_id=${activeSensor}`;
  
  // let station = $('#station_name').text();
  let station = $('#station_name').text().toLowerCase().replace(/ /g, '_');
  console.log(station);
  var url = `fetch_history.php?station=${station}`;

  fetch(url)
    .then(response => response.json())
    .then(data => {
      console.log('Fetched initial data:', data);

      // Check if chart already exists
      if (chart) {
        chart.dispose(); // Dispose of the old chart if necessary
      }

      // Create the chart instance
      chart = am4core.create("chartdiv", am4charts.XYChart);

      // Set the data for the chart
      chart.data = data;

      // Update lastId if data exists
      if (data.length > 0) {
        lastId = Math.max(...data.map(d => d.id));
        console.log('Initial lastId:', lastId);
      }

      // Create date axis
      var dateAxis = chart.xAxes.push(new am4charts.DateAxis());
      dateAxis.renderer.grid.template.location = 0;
      dateAxis.dateFormatter.inputDateFormat = "yyyy-MM-dd HH:mm:ss";
      dateAxis.renderer.labels.template.fontSize = 10;
      dateAxis.renderer.labels.template.fill = am4core.color("#FFFFFF");
      dateAxis.baseInterval = { timeUnit: "second", count: 1 };
      dateAxis.tooltipDateFormat = "yyyy-MM-dd HH:mm:ss";

      // Create value axis
      var valueAxis = chart.yAxes.push(new am4charts.ValueAxis());
      valueAxis.renderer.labels.template.fontSize = 10;
      valueAxis.renderer.labels.template.fill = am4core.color("#FFFFFF");

      // Function to create a series for each data field
      var createSeries = function (field, name) {
        // Map field names to display names
        const nameMappings = {
          water_level: "Water Level"
        };

        if (nameMappings[field]) {
          name = nameMappings[field];
        }

        var series = chart.series.push(new am4charts.LineSeries());
        series.dataFields.valueY = field;
        series.dataFields.dateX = "timestamp";
        series.name = name;
        series.tooltipText = "{name}: [bold]{valueY}[/]";
        series.strokeWidth = 2;
        series.strokeDasharray = "5,2";
        series.tensionX = 0.9;

        var bullet = series.bullets.push(new am4charts.CircleBullet());
        bullet.circle.radius = 2;
        bullet.circle.strokeWidth = 1;
        bullet.circle.fill = am4core.color("#fff");

        // Set series colors based on field
        const fieldColors = {
          water_level: "aqua"
        };

        if (fieldColors[field]) {
          series.stroke = am4core.color(fieldColors[field]);
        }

        return series;
      };

      // Create series for all fields except "timestamp" and "id"
      for (var key in data[0]) {
        if (key !== "timestamp" && key !== "id") {
          createSeries(key, key);
        }
      }

      // Add legend 
      chart.legend = new am4charts.Legend();
      chart.legend.fontSize = 12;
      chart.legend.labels.template.fill = am4core.color("#FFFFFF");

      // Make legend responsive
      if (window.matchMedia("(max-width: 767px)").matches) {
        chart.legend.itemContainers.template.layout = "vertical";
        chart.legend.itemContainers.template.columnCount = 1;
      }

      // Add cursor and export menu
      chart.cursor = new am4charts.XYCursor();
      chart.mouseWheelBehavior = "zoomX";
      // chart.exporting.menu = new am4core.ExportMenu();
    })
    .catch(error => console.error('Error fetching initial data:', error));
}

// Set an interval to fetch the latest data periodically
// setInterval(fetchLatestData, 5000); // Fetch every 5 seconds
</script>
