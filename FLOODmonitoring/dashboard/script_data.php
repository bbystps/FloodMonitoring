<script>
function loadData() {
  
  let station = $('#station_name').text().toLowerCase().replace(/ /g, '_');
  // console.log(location_ref);
  // console.log(station);
  $.ajax({
    url: 'fetch_data.php', // Path to your PHP script
    method: 'GET',
    dataType: 'json',
    data: { station: station },
    success: function(response) {
      // console.log(response);
      $('#WL_last').text(response[0].water_level);
      $('#rssi_val').text(response[0].rssi_val);
      $('#snr').text(response[0].snr);
      $('#packet_rec_ratio').text(response[0].packet_rec_ratio);
      $('#batt_voltage').text(response[0].battery_voltage);
    },
    error: function(xhr, status, error) {
      console.error('Error fetching data:', xhr.responseText); // Log the actual server response
    }
  });
}
</script>