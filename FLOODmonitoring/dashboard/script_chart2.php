<script>
var chart; // Define chart as a global variable

function updateChart() {
  // Dispose of the previous chart if it exists
  if (chart) {
    chart.dispose();
  }

  // let station = $('#station_name').text();
  let station = $('#station_name').text().toLowerCase().replace(/ /g, '_');
  console.log(station);
  // Construct the URL with or without sensorId
  var url = `fetch_history.php?station=${station}`;

  fetch(url)
    .then(response => response.json())
    .then(data => {
      console.log('Fetched data:', data); // Log the fetched data

      // Create the chart instance
      chart = am4core.create("chartdiv", am4charts.XYChart);

      // Set the data for the chart
      chart.data = data;

      // Create date axis
      var dateAxis = chart.xAxes.push(new am4charts.DateAxis());
      dateAxis.renderer.grid.template.location = 0;
      dateAxis.dateFormatter.inputDateFormat = "yyyy-MM-dd HH:mm:ss";

      // Set the font size for the date axis
      dateAxis.renderer.labels.template.fontSize = 10;
      dateAxis.renderer.labels.template.fill = am4core.color("#FFFFFF");

      // Set the base interval to one second
      dateAxis.baseInterval = { timeUnit: "second", count: 1 };

      // Specify the format for the tooltip
      dateAxis.tooltipDateFormat = "yyyy-MM-dd HH:mm:ss"; 

      // Create value axis
      var valueAxis = chart.yAxes.push(new am4charts.ValueAxis());

      // Set the font size for the value axis
      valueAxis.renderer.labels.template.fontSize = 10;
      valueAxis.renderer.labels.template.fill = am4core.color("#FFFFFF");


      // Create series for each line
      var createSeries = function(field, name) {

        if (name === "water_level") {
            name = "Water Level";
        }

        var series = chart.series.push(new am4charts.LineSeries());
        series.dataFields.valueY = field;
        series.dataFields.dateX = "timestamp"; // Adjust to your date field
        series.name = name;
        series.tooltipText = "{name}: [bold]{valueY}[/]";
        series.strokeWidth = 2;

        // Set the line stroke to dotted
        series.strokeDasharray = "5,2";
        // Enable smoothing
        series.tensionX = 0.99;  // Adjust this value to control the amount of curvature

        var bullet = series.bullets.push(new am4charts.CircleBullet());
        bullet.circle.radius = 2;
        bullet.circle.strokeWidth = 1;
        bullet.circle.fill = am4core.color("#fff");
        
        // Conditional coloring based on field name
        if (field === "water_level") {
          series.stroke = am4core.color("aqua"); // Change color to red for CURRENT-LOADS
        }

        return series;
      };

      // Iterate through keys in the first item of the response and create series
      for (var key in data[0]) {
        if (key !== "timestamp" && key !== "id") { // Exclude the date field
          createSeries(key, key); // Use key as both field and series name
        }
      }

      // Add legend
      chart.legend = new am4charts.Legend();
      chart.legend.fontSize = 10; // Set the font size for the legend items
      // Set legend label color
      chart.legend.labels.template.fill = am4core.color("#FFFFFF"); // Change to your preferred color

      // Use media query to make the legend items flex on mobile screens
      if (window.matchMedia("(max-width: 767px)").matches) {
        chart.legend.itemContainers.template.layout = "vertical";
        chart.legend.itemContainers.template.columnCount = 1;
      }

      // Add cursor
      chart.cursor = new am4charts.XYCursor();
      chart.mouseWheelBehavior = "zoomX";

      // Enable export
      chart.exporting.menu = new am4core.ExportMenu();

      // Enable scrollbar
      //chart.scrollbarX = new am4core.Scrollbar();
    })
    .catch(error => console.error('Error fetching data:', error));
}
</script>
