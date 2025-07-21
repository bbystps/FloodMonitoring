<script>
let barChart;
let hand; // This will represent the dynamic value
let max = 0; // Declare max as a global variable
let min = 0;
let value1, endValue1, value2, endValue2, value3, endValue3, value4, endValue4;

function createVerticalBarChart() {
  
  let station = $('#station_name').text().toLowerCase().replace(/ /g, '_');
  
  // Adjust the color ranges based on the station
  if (station == 'brgy_nueva_era') {
    console.log("Create Gauge for Brgy Nueva Era");
    max = 13;
    value1 = 0;
    endValue1 = 0.25;
    value2 = 0.35;
    endValue2 = 3.4;
    value3 = 3.5;
    endValue3 = 7.9;
    value4 = 8;
    endValue4 = 13;
  } else if (station == 'brgy_san_marcos') {
    console.log("Create Gauge for Brgy San Marcos");
    max = 10;
    value1 = 0;
    endValue1 = 0.25;
    value2 = 0.35;
    endValue2 = 2;
    value3 = 2.1;
    endValue3 = 5;
    value4 = 5.1;
    endValue4 = 10;
  }

  // Create bar chart
  barChart = am4core.create("gaugediv", am4charts.XYChart);
  barChart.hiddenState.properties.opacity = 0; // makes chart fade-in on load

  // Create a value axis (vertical) for the progress
  let valueAxis = barChart.yAxes.push(new am4charts.ValueAxis());
  valueAxis.min = min;
  valueAxis.max = max;
  valueAxis.strictMinMax = true;
  valueAxis.renderer.opposite = true;
  valueAxis.renderer.grid.template.disabled = true;
  valueAxis.renderer.labels.template.fill = am4core.color("#ffffff"); // Label color
  valueAxis.renderer.labels.template.fontSize = 12; // Label font size

  // Create a category axis (horizontal, just for the layout)
  let categoryAxis = barChart.xAxes.push(new am4charts.CategoryAxis());
  categoryAxis.dataFields.category = "category";
  categoryAxis.renderer.grid.template.disabled = true;
  categoryAxis.renderer.labels.template.disabled = true;

  // Create the series for the bar
  let series = barChart.series.push(new am4charts.ColumnSeries());
  series.dataFields.valueY = "value";
  series.dataFields.categoryX = "category";
  series.columns.template.strokeWidth = 0;

  // Set the initial data for the bar
  barChart.data = [
    { "category": "Bar", "value": 0 }
  ];

  // Add color ranges for each segment
  let range1 = valueAxis.axisRanges.create();
  range1.value = value1;
  range1.endValue = endValue1;
  range1.axisFill.fill = am4core.color("#00FF00"); // Green
  range1.axisFill.fillOpacity = 0.7;

  let range2 = valueAxis.axisRanges.create();
  range2.value = value2;
  range2.endValue = endValue2;
  range2.axisFill.fill = am4core.color("#FFFF00"); // Yellow
  range2.axisFill.fillOpacity = 0.7;

  let range3 = valueAxis.axisRanges.create();
  range3.value = value3;
  range3.endValue = endValue3;
  range3.axisFill.fill = am4core.color("#FFA500"); // Orange
  range3.axisFill.fillOpacity = 0.7;

  let range4 = valueAxis.axisRanges.create();
  range4.value = value4;
  range4.endValue = endValue4;
  range4.axisFill.fill = am4core.color("#FF0000"); // Red
  range4.axisFill.fillOpacity = 0.7;

  // Create the hand (pointer) indicator
  hand = barChart.createChild(am4core.Label);
  hand.text = "▶"; // Arrow pointing to the right
  hand.fontSize = 30;
  hand.fill = am4core.color("#FFFFFF"); // Default to white, change dynamically based on value
  hand.align = "left";
  hand.dx = -20; // Move it slightly to the right, adjust based on your chart's position
  hand.verticalCenter = "middle"; // Center it vertically with the bar
  hand.rotation = 0; // Ensure no rotation (it points right by default)
  
  // Add a hand indicator (pointer)
  // hand = barChart.createChild(am4core.Label);
  // hand.text = "▼"; // Arrow pointing to the current value
  // hand.fontSize = 20;
  // hand.fill = am4core.color("#FFFFFF"); // Default to white, change dynamically based on value
  // hand.align = "left";
  // hand.dy = -10; // Position it above the bar
  
  // updateVerticalBar(5, endValue1, endValue2, endValue3, endValue4); // Initialize at 0, pass ranges
}

// Function to update the vertical bar based on the value and change color
function updateVerticalBar(value, endValue1, endValue2, endValue3, endValue4) {
  if (hand) { // Ensure hand is initialized
    // Update the bar height (value)
    // barChart.data[0].value = value;
    console.log("Passed Value: " + value);
    // Adjust the hand (pointer) position

    hand.dy = -8 - (value / max) * 338; // Adjust dy based on the value and max

    // Adjust hand color dynamically based on the value
    if (value <= endValue1) {
      // hand.fill = am4core.color("#00FF00"); // Green for normal
      $('#SD_status').text("Normal");
    } else if (value <= endValue2) {
      // hand.fill = am4core.color("#FFFF00"); // Yellow for caution
      $('#SD_status').text("Caution");
    } else if (value <= endValue3) {
      // hand.fill = am4core.color("#FFA500"); // Orange for warning
      $('#SD_status').text("Warning");
    } else {
      // hand.fill = am4core.color("#FF0000"); // Red for danger
      $('#SD_status').text("Danger");
    }

    // No invalidation of the chart to prevent reset or blue overlay
  }
}

// Function to fetch data and update the bar using jQuery AJAX
function fetchAndUpdateBar() {
  let station = $('#station_name').text().toLowerCase().replace(/ /g, '_');
  
  $.ajax({
    url: 'fetch_smoke_detector.php',
    type: 'GET',
    data: {
      station: station
    },
    success: function(response) {
      var newValue = parseFloat(response.water_level);
      if (!isNaN(newValue) && newValue <= max) {
        updateVerticalBar(newValue, endValue1, endValue2, endValue3, endValue4); // Pass range values here
      } else {
        // updateVerticalBar(0, endValue1, endValue2, endValue3, endValue4); // Set to 0 and pass ranges
        console.warn('No valid data received. Bar set to 0.');
      }
    },
    error: function(xhr, status, error) {
      updateVerticalBar(0, endValue1, endValue2, endValue3, endValue4); // Pass range values in case of error
      console.error('Error fetching data:', error);
    }
  });
}
</script>
