<script>
function loadCards() {
  console.log("load cards");
  $.ajax({
    url: 'fetch_cards.php', // Path to your PHP script
    method: 'GET',
    dataType: 'json',
    success: function(response) {
      $('#total_devices').text(response.totalDevices);
      $('#offline_devices').text(response.offlineDevices);
      $('#total_alarms').text(response.totalAlarms);
      $('#needs_maintenance').text(response.needsMaintenance);
    },
    error: function(xhr, status, error) {
      console.error('AJAX Error:', status, error);
    }
  });
}
</script>