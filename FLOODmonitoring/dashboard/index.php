<!DOCTYPE html>
<html lang="en">
  
<?php 
  session_start();
  if($_SESSION['loggedin'] !== true){
    header("Location: ../login");
    exit();
  }
?>

<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Real-Time Flood Monitoring</title>
  
  <link rel="stylesheet" href="../css/montserrat.css">
  <link rel="stylesheet" href="../css/icon.css">
  <link rel="stylesheet" href="../css/element.css">
  <link rel="stylesheet" href="../css/style.css">
  <!-- <link rel="stylesheet" href="../datatables/css/dataTables.dataTables.min.css"> -->
  <link rel="stylesheet" href="https://unpkg.com/leaflet/dist/leaflet.css" />
  <script src="https://unpkg.com/leaflet/dist/leaflet.js"></script>
  
  <link rel="stylesheet" href="toastr/toastr.min.css">
</head>
<script src="mqtt/mqttws31.js"></script>
<?php include("mqtt/cards_mqtt.php"); ?>

<body onload="client.connect(options);">
  
<div class="container">

  <div id="nav-hidden">
    <div class="container-space-between">
      <!-- Toggle button -->
      <!-- <div class="toggle-button">☰</div> -->
      <div class="toggle-button">&#9776;</div>
      <div class="title-nav-hidden">Real-Time Flood Monitoring</div>
      <div class="user-button">
        <div class="user-container">
          <div class="user-text">
            Admin
            <i class="icon-user"></i> 
            <div class="logout-button" onclick="logout();">Logout</div> <!-- Logout button -->
          </div>
        </div>
      </div>
    </div>
  </div>

  <!-- user - put top details here -->
  <div id="user">
    <div class="user-container">
      <div class="user-text">
        Admin
        <i class="icon-user"></i> 
        <div class="logout-button" onclick="logout();">Logout</div> <!-- Logout button -->
      </div>
    </div>
  </div>
  
  <div id="nav">
    <div class="nav-container">
      <div class="nav-lists nav-active" onclick="gotoDashboard();">Dashboard</div>
      <!-- <div class="nav-lists nav-active" onclick="fetchLatestData();">Fetch New</div> -->
      <!-- <div class="nav-lists" onclick="gotoDevices();">Devices</div> -->
      <!-- <div class="nav-lists" onclick="gotoAlarmLogs();">Alarm Logs</div>
      <div class="nav-lists" onclick="gotoSettings();">Settings</div> -->
    </div>
  </div>


  <!-- mainbar - put cards dashboard here -->
  <div id="main">
    <div class="main-container">
      <div class="main-card">
        <div class="main-card-num" id="total_devices">0</div>
        <div class="main-card-text">
          <div class="main-card-text-up">Total</div>
          <div class="main-card-text-down">Devices</div>
        </div>
      </div>
      <div class="main-card">
        <div class="main-card-num" id="offline_devices">0</div>
        <div class="main-card-text">
          <div class="main-card-text-up">Offline</div>
          <div class="main-card-text-down">Devices</div>
        </div>
      </div>
      <div class="main-card">
        <div class="main-card-num" id="total_alarms">0</div>
        <div class="main-card-text">
          <div class="main-card-text-up">Total</div>
          <div class="main-card-text-down">Alarms</div>
        </div>
      </div>
      <div class="main-card">
        <div class="main-card-num" id="needs_maintenance">0</div>
        <div class="main-card-text">
          <div class="main-card-text-up">Needs</div>
          <div class="main-card-text-down">Maintenance</div>
        </div>
      </div>
    </div>
  </div> 

  <!-- sidebar start -->
  <div id="sidebar">
    <?php include("../includes/sidebar.php"); ?>
  </div>
  <!-- sidebar end -->

  <!-- content1 - historical for sensors val -->
  <div id="content1">
    <div class="content1-container">
      <div class="content1-label">
        <!-- <div id="sensor_id"></div> -->
        <div class="container-flex">
          <div id="station_name">Station 1</div>
          <div class="content1-date" id="date_today"><?php echo date('l, F j, Y');?></div>
          <div class="content1-btn">
            <button class="btn-small pad3" id="historyDownload" onclick="downloadHistory()">Download History</button>
            <button class="btn-small pad3" id="historyReset" onclick="resetHistory()">Reset</button>
          </div>
        </div>
      </div>
      <div id="chartdiv">chart placed here</div>
    </div>
  </div>

  <!-- content2 - historical value for battery -->
  <div id="content2">
    <div class="content2-container">
      
      <div class="content2-buttons">
        <div class="content2-title">Alarm System</div> 
        <p id="btn-status"></p>
        <div class="content2-btn">
          <div class="horn-container">
            <div class="horn-button horn-color-yellow" id="horn-ylw-btn"><i class="horn-fill" onclick="alarm_btn('01');"></i></div>
            <div class="horn-name">Level 1</div>
          </div>
          <div class="horn-container">
            <div class="horn-button horn-color-orange" id="horn-orn-btn"><i class="horn-fill" onclick="alarm_btn('02');"></i></div>
            <div class="horn-name">Level 2</div>
          </div>
          <div class="horn-container">
            <div class="horn-button horn-color-red" id="horn-red-btn"><i class="horn-fill" onclick="alarm_btn('03');"></i></div>
            <div class="horn-name">Level 3</div>
          </div>
        </div>
      </div>

      <div class="content2-card">
        <div class="container-left">
          <div class="signal-quality">
            <h2>Signal Quality</h2>
            <h3>RSSI value: <span id="rssi_val"></span></h3>
            <h3>SNR: <span id="snr"></span></h3>
            <h3>Packet Rec Ratio: <span id="packet_rec_ratio"></span></h3>
          </div>
          <div class="battery">
            <h2>Battery</h2>
            <h3>Voltage: <span id="batt_voltage"></span></h3>
          </div>
          <div class="download-button">
            <button id="downloadExcel" onclick="downloadExcel()">Download Data</button>
          </div>
        </div>
      </div>

    </div>
  </div>

  <!-- content3 - smoke sensor and map (Normal(grn), Caution(yellow), Warning(orng), Danger(red))-->
  <div id="content3">
    <div class="content3-card-container">
      <div class="content3-card">
        <div class="content1-title">Flood Water Level: <span id="WL_last"></span> ft</div>
        <!-- <div class="content3-card-sensorid" id="SD_display">SD-ID</div> -->
        <div class="content3-card-gauge" id="gaugediv"></div>
        <div class="content3-card-status" id="SD_status">NORMAL</div> 
      </div>
      
    </div>
  </div>

</div>

<?php include("modal.php"); ?>
<?php include("../includes/admin_modal.php"); ?>

</body>

<script src="../js/jquery.min.js"></script>
<script src="toastr/toastr.min.js"></script>

<script src="https://cdn.amcharts.com/lib/4/core.js"></script>
<script src="https://cdn.amcharts.com/lib/4/charts.js"></script>
<script src="https://cdn.amcharts.com/lib/4/themes/animated.js"></script>

<?php include("script_chart.php"); ?>
<?php include("script_gauge.php"); ?>
<?php include("script_data.php"); ?>
<?php include("script_cards.php"); ?>
<?php //include("script_modal.php"); ?>

<script>
  function resetHistory() {
    if (confirm("Are you sure you want to reset the database? This action cannot be undone.")) {
      let station = $('#station_name').text().toLowerCase().replace(/ /g, '_');
      fetch(`db_reset.php?station=${station}`, {
        method: 'POST',
      })
      .then(response => response.json())
      .then(data => {
        if (data.success) {
          alert("Database reset successfully.");
        } else {
          alert("Failed to reset the database: " + data.message);
        }
      })
      .catch(error => {
        console.error("Error resetting database:", error);
        alert("An error occurred while resetting the database.");
      });
    }
  }

  async function downloadHistory() {
    try {
      let station = $('#station_name').text().toLowerCase().replace(/ /g, '_');
      const response = await fetch(`download_history.php?station=${station}`);
      if (!response.ok) {
        throw new Error('Failed to fetch data');
      }

      const data = await response.json();

      if (data.length === 0) {
        alert('No data available to export.');
        return;
      }

      // Prepare CSV content
      const rows = [
        ['Water Level', 'Timestamp'], // Header row
        ...data.map(row => [row.water_level, row.timestamp]) // Data rows
      ];

      const csvContent = rows
        .map(row => row.map(value => `"${value}"`).join(','))
        .join('\n');

      const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
      const url = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.setAttribute('download', "History_" + `${station}.csv`);
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
    } catch (error) {
      console.error('Error downloading Water Level Excel:', error);
    }
  }


  async function downloadExcel() {
    try {
      let station = $('#station_name').text().toLowerCase().replace(/ /g, '_');
      const response = await fetch(`download_excel.php?station=${station}`);
      if (!response.ok) {
        throw new Error('Failed to fetch data');
      }

      const data = await response.json();

      if (data.length === 0) {
        alert('No data available to export.');
        return;
      }

      // Generate Excel file
      const rows = [
        ['Water Level', 'RSSI Value', 'SNR', 'Packet Rec Ratio', 'Battery Voltage', 'Timestamp'],
        ...data.map(row => [
          row.water_level,
          row.rssi_val,
          row.snr,
          row.packet_rec_ratio,
          row.battery_voltage,
          row.timestamp
        ])
      ];

      const csvContent = rows
        .map(row => row.map(value => `"${value}"`).join(','))
        .join('\n');

      const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
      const url = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.setAttribute('download', "SQ_" + `${station}.csv`);
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
    } catch (error) {
      console.error('Error downloading Excel:', error);
    }
  }
</script>

<script>
  function alarm_btn(payload_data){
    
    let station = $('#station_name').text().toLowerCase().replace(/ /g, '_');
    console.log(station);
    console.log(payload_data);
    const btn_status = document.getElementById('btn-status');
    btn_status.innerHTML = "Connecting to device ...";

    const buttons = [
        document.getElementById('horn-ylw-btn'),
        document.getElementById('horn-orn-btn'),
        document.getElementById('horn-red-btn')
    ];

    // Disable button function
    function disableButton(button) {
        button.style.pointerEvents = 'none'; // Prevent further clicks
        button.style.opacity = '0.5'; // Change appearance to indicate it's disabled
    }

    // Disable all buttons
    buttons.forEach(disableButton); // Disable all buttons in the array
    console.log("alarm one");

    // Data to send (e.g., 01)
    // const data = { payload: payload_data };
    
    const data = { 
      payload: payload_data, 
      station: station // Add station value to the data object
    };

    // Send the AJAX request
    $.ajax({
      url: 'downlink.php',  // PHP file to handle downlink
      type: 'POST',
      data: data,
      success: function(response) {
        console.log('Downlink response:', response);
      },
      error: function(xhr, status, error) {
        console.log('Error:', error);
      }
    });
  }
</script>


<script>
  const ws = new WebSocket("ws://3.27.210.100:5001");
  
  ws.onopen = () => {
    console.log("WebSocket connected");
    //document.getElementById("status").innerText = "WebSocket connected.";
  };

  ws.onmessage = (event) => {
    // Parse incoming message and display it
    try {
      const btn_status = document.getElementById('btn-status');
      const data = JSON.parse(event.data);
      // const messageDiv = document.createElement("div");
      //messageDiv.innerText = `Acknowledgment - Confirmed: ${data.confirmed}, Frame Count: ${data.f_cnt}, Payload: ${data.decoded_payload}, Attempt: ${data.attempt}`;
      //document.getElementById("messages").appendChild(messageDiv);
      if (data.confirmed === true || data.confirmed === "true") {
        console.log("Confirmed acknowledgment received.");
        const btn_status = document.getElementById('btn-status');
        btn_status.innerHTML = "";
            
        const buttons = [
          document.getElementById('horn-ylw-btn'),
          document.getElementById('horn-orn-btn'),
          document.getElementById('horn-red-btn')
        ];

        // Enable button function
        function enableButton(button) {
          button.style.pointerEvents = 'auto'; // Prevent further clicks
          button.style.opacity = '1'; // Change appearance to indicate it's disabled
        }
        // Enable all buttons
        buttons.forEach(enableButton); // Disable all buttons in the array
      }

    } catch (error) {
      console.error("Error parsing WebSocket message:", error);
    }
  };

  ws.onerror = (error) => {
    console.error("WebSocket error:", error);
    document.getElementById("status").innerText = "WebSocket error.";
  };

  ws.onclose = () => {
    console.log("WebSocket closed");
    document.getElementById("status").innerText = "WebSocket disconnected.";
  };
</script>

<script>
const userTexts = document.querySelectorAll('.user-text');
const logoutButtons = document.querySelectorAll('.logout-button');
// Add click event listeners to each user text
userTexts.forEach((userText, index) => {
  userText.addEventListener('click', (event) => {
    // Prevent the event from bubbling up to the document
    event.stopPropagation();
    
    // Toggle the corresponding logout button
    logoutButtons[index].style.display = (logoutButtons[index].style.display === 'block') ? 'none' : 'block';
  });
});
// Hide the logout button if clicking outside
document.addEventListener('click', () => {
  logoutButtons.forEach(button => {
    button.style.display = 'none';
  });
});
</script>

<script>

$(document).ready(function(){
  $.ajax({
    url: 'fetch_site_locations.php', // Path to your PHP script
    method: 'GET',
    dataType: 'json',
    success: function(response) {
      // Clear the existing content
      $('.site-lists').empty();

      // Loop through the response and append to the site-lists div
      response.forEach(function(location, index) {
        // Set the first location as active by checking if the index is 0
        const isActive = index === 0 ? ' active' : '';
        $('.site-lists').append(
          '<p><i class="icon-location"></i><span class="site-location' + isActive + '" data-station="' + location.station + '">' + location.station + '</span></p>'
        );
      });

      // Add click event listener to change active region
      $('.site-lists').on('click', '.site-location', function() {
        // Remove active class from all regions
        $('.site-location').removeClass('active');
        
        // Add active class to the clicked region
        $(this).addClass('active');
        const selectedStation = $(this).data('station');
        console.log('Selected Station:', selectedStation);
        $('#station_name').text(selectedStation);
        console.log("Change Location");
        updateChart();
        // createGaugeChart();
        // fetchAndUpdateGauge();
        fetchAndUpdateBar();
        loadData();
        loadCards();
      });

      // Set the first station as the selected station by default
      if(response.length > 0) {
        const firstStation = response[0].station;
        $('#station_name').text(firstStation);
        // getLocationSensor();
        updateChart();      
        // createGaugeChart(); // Initialize the gauge chart
        createVerticalBarChart();
        // fetchAndUpdateGauge();
        fetchAndUpdateBar();
        loadData();
        loadCards();
      }
    },
    error: function(xhr, status, error) {
      console.error('AJAX Error:', status, error);
    }
  });
});
</script>

<!-- Sidebar toggle button open close -->
<script>
document.querySelector('.toggle-button').addEventListener('click', function() {
  const sidebar = document.getElementById('sidebar');
  sidebar.classList.add("sidebar-visible");
  sidebar.style.display = 'block';
});
document.querySelector('.close-sidebar').addEventListener('click', function() {
  const sidebar = document.getElementById('sidebar');
  sidebar.classList.remove("sidebar-visible");
  sidebar.style.display = 'none';
});
window.addEventListener('resize', function() {
  const sidebar = document.getElementById('sidebar');
  if (window.innerWidth > 550) {
    // Automatically hide the sidebar on larger screens
    sidebar.classList.remove("sidebar-visible");
    sidebar.style.display = 'block';
  } else if (!sidebar.classList.contains("sidebar-visible")) {
    sidebar.style.display = 'none';
  }
});
</script>

<script>
  function gotoDashboard(){window.location.replace("../dashboard");}
  function gotoDevices(){window.location.replace("../devices");}
  function gotoAlarmLogs(){window.location.replace("../alarm logs");}
  function gotoSettings(){window.location.replace("../settings");}
  function logout(){window.location.replace("../login/logout.php");}
</script>
</html>
