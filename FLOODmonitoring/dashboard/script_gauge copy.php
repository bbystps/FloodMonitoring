<script>
$(document).ready(function() {
  
  $('.searchInput').on('keyup', function() {
    var value = $(this).val().toLowerCase();

    // If search input is not empty, filter the table
    if (value) {
      $('#sensor-data tr').filter(function() {
        $(this).toggle($(this).text().toLowerCase().indexOf(value) > -1);
      });
    } else {
      // If search input is empty, reset and reapply pagination
      $('#sensor-data tr').show(); // Show all rows
      paginationFunction(); // Reapply pagination to show only the first page
    }
  });
});

let gaugeChart;
let hand; // Define hand as a global variable

function createGaugeChart() {
  
  let station = $('#station_name').text().toLowerCase().replace(/ /g, '_');
  
  let min = 0;
  let max = 20; // You can adjust this as a general max value
  let value1 = 0;
  let endValue1 = 5;
  let value2 = 5.1;
  let endValue2 = 10;
  let value3 = 10.1;
  let endValue3 = 15;
  let value4 = 15.1;
  let endValue4 = 20;

  if (station == 'brgy_nueva_era') {
    console.log("Create Gauge for Brgy Nueva Era");
    min = 0; //min value
    max = 13; //max value
    value1 = 0; //green min
    endValue1 = 0.25; //green max
    value2 = 0.35; //yellow min
    endValue2 = 3.4; //yellow max
    value3 = 3.5; //orange min
    endValue3 = 7.9; //orange max
    value4 = 8; //red min
    endValue4 = 13; //red max
  } else if (station == 'brgy_san_marcos') {
    console.log("Create Gauge for Brgy San Marcos");
    min = 0;
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

  gaugeChart = am4core.create("gaugediv", am4charts.GaugeChart);
  
  gaugeChart.innerRadius = am4core.percent(90);
  
  var axis = gaugeChart.xAxes.push(new am4charts.ValueAxis());
  axis.min = min;
  axis.max = max;
  axis.strictMinMax = true;
  axis.renderer.radius = am4core.percent(100);
  axis.renderer.startAngle = -90;
  axis.renderer.endAngle = 90;
  axis.renderer.grid.template.disabled = true;
  axis.renderer.labels.template.fill = am4core.color("#ffffff"); // Label color
  axis.renderer.labels.template.fontSize = 8; // Label font size
  
  var colorSet = new am4core.ColorSet();
  
  var range1 = axis.axisRanges.create();
  range1.value = value1;
  range1.endValue = endValue1;
  range1.axisFill.fill = am4core.color("#ffffff"); // Green
  range1.axisFill.fillOpacity = 0.7;
  range1.label.fill = am4core.color("#ffffff");
  range1.label.fontSize = 8;
  range1.label.horizontalCenter = "middle";
  range1.label.verticalCenter = "middle";
  range1.label.dy = 13; // Adjusted for better placement

  var range2 = axis.axisRanges.create();
  range2.value = value2;
  range2.endValue = endValue2;
  range2.axisFill.fill = am4core.color("#FFFF00"); // Yellow
  range2.axisFill.fillOpacity = 0.7;
  range2.label.fill = am4core.color("#000000");
  range2.label.fontSize = 14;
  range2.label.horizontalCenter = "middle";
  range2.label.verticalCenter = "middle";
  range2.label.dy = 20;

  var range3 = axis.axisRanges.create();
  range3.value = value3;
  range3.endValue = endValue3;
  range3.axisFill.fill = am4core.color("#FFA500"); // Orange
  range3.axisFill.fillOpacity = 0.7;
  range3.label.fill = am4core.color("#000000");
  range3.label.fontSize = 14;
  range3.label.horizontalCenter = "middle";
  range3.label.verticalCenter = "middle";
  range3.label.dy = 20;

  var range4 = axis.axisRanges.create();
  range4.value = value4;
  range4.endValue = endValue4;
  range4.axisFill.fill = am4core.color("#FF0000"); // Red
  range4.axisFill.fillOpacity = 0.7;
  range4.label.fill = am4core.color("#ffffff");
  range4.label.fontSize = 14;
  range4.label.horizontalCenter = "middle";
  range4.label.verticalCenter = "middle";
  range4.label.dy = 20;
  
  hand = gaugeChart.hands.push(new am4charts.ClockHand()); // Initialize hand
};

// Function to update hand color based on value
function updateHandColor(value) {
  if (hand) { // Ensure hand is initialized
    if (value <= 2.9) {
      hand.stroke = am4core.color("#ffffff"); // Green
      $('#SD_status').text("Normal");
    } else if (value <= 3.5) {
      hand.stroke = am4core.color("#FFFF00"); // Yellow
      $('#SD_status').text("Caution");
    } else if (value <= 7.9) {
      hand.stroke = am4core.color("#FFA500"); // Orange
      $('#SD_status').text("Warning");
    } else {
      hand.stroke = am4core.color("#FF0000"); // Red
      $('#SD_status').text("Danger");
    }
  }
}

// Function to fetch data and update gauge using jQuery AJAX
function fetchAndUpdateGauge() {
  // console.log('Fetching data for sensor ID:', sensorId); // Debugging line
  // let station = $('#station_name').text();
  let station = $('#station_name').text().toLowerCase().replace(/ /g, '_');
  //console.log("GAUGE: " + station);
  $.ajax({
    url: 'fetch_smoke_detector.php',
    type: 'GET',
    data: {
      station: station
    },
    success: function(response) {
      // console.log('Data received:', response); // Debugging line
      //console.log("response" + response);
      var newValue = parseFloat(response.water_level);
      if (!isNaN(newValue)) { // Check if newValue is a valid number
        hand.showValue(newValue, 1000);
        updateHandColor(newValue);
        // $('#SD_display').text(response.sensor_id);
        // $('#HV_display').text(response.sensor_id);
        
      } else {
        // console.error('Invalid smoke_detector data received:', response.smoke_detector); // Debugging line
        // Set gauge to 0 if the response data is invalid
        hand.showValue(0, 1000);
        updateHandColor(0);
        // $('#SD_display').text('N/A');
        // $('#HV_display').text('N/A');
        console.warn('No valid data received. Gauge set to 0.');
      }
    },
    error: function(xhr, status, error) {
      // console.error('Error fetching data:', error); // Debugging line

      hand.showValue(0, 1000);
      updateHandColor(0);
      // $('#SD_display').text('N/A');
      // $('#HV_display').text('N/A');
      console.error('Error fetching data:', error);
    }
  });
}
</script>