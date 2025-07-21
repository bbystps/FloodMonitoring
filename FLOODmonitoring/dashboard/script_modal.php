
<!-- Modal Script Start -->
<script>

function openSuccessModal() {
  const successModal = document.getElementById('successModal');
  successModal.style.display = 'flex';
}
function closeSuccessModal() {
  const successModal = document.getElementById('successModal');
  successModal.style.display = 'none';
}
function openErrorModal() {
  const errorModal = document.getElementById('errorModal');
  errorModal.style.display = 'flex';
}
function closeErrorModal() {
  const errorModal = document.getElementById('errorModal');
  errorModal.style.display = 'none';
}

function openAddDeviceModal() {
  $("#NEWdeviceFORM")[0].reset();
  const AddDeviceModal = document.getElementById('AddDeviceModal');
  AddDeviceModal.style.display = 'flex';
}
function closeAddDeviceModal() {
  const AddDeviceModal = document.getElementById('AddDeviceModal');
  AddDeviceModal.style.display = 'none';
}

function confirmAddDeviceModal(){
  console.log("Adding New Device");
  var form = $('#NEWdeviceFORM')[0];
  var data = new FormData(form);
  //data.append('EMP_ID', document.getElementById('session_id').value);
  $.ajax({
    type: "POST",
    enctype: 'multipart/form-data',
    url:"add_new_device.php",
    data:data,
    processData: false,
    contentType: false,
    cache: false,
    success:function(data){
      try {
      // If the data is already a JSON object, you don't need to parse it.
      // But if it's a string, you can parse it using JSON.parse()
      var responseData = typeof data === 'string' ? JSON.parse(data) : data;

      // Access the parsed data
      if(responseData.status === 'success'){
        console.log(responseData.message); // Outputs: Device added and tables created successfully
        closeAddDeviceModal();
        $("#promptSuccessSM").text("Device Added Successfully.");
        openSuccessModal();
      } else {
        console.log("Error:", responseData.message);
        closeAddDeviceModal();
        $("#promptErrorSM").text("Failed to Add Device");
        openErrorModal();
      }
    } catch (e) {
      console.error("Parsing error:", e);
      closeAddDeviceModal();
      $("#promptErrorSM").text("Failed to Add Device");
      openErrorModal();
    }
    },
    error: function(xhr, status, error) {
    console.error(xhr.responseText);
    $("#promptErrorSM").text("Failed to Add Device");
    openErrorModal();
    }
  })
}

function loadAreas() {
  const region = $('#region').val();
  const areaSelect = $('#area');
  areaSelect.html('<option value="">--Select an Area--</option>'); // Reset areas

  if (region) {
    $.ajax({
      type: 'GET',
      url: 'json_data/regions_areas.json',
      dataType: 'json',
      success: function(data) {
        const areas = data[region] || []; // Get areas for the selected region
        areas.forEach(area => {
          areaSelect.append(new Option(area, area));
        });
      },
      error: function() {
        console.error('Failed to load region-area mapping.');
      }
    });
  }
}

let regionAcronyms = {};

// Load the regions_db.json file
fetch('json_data/regions_id.json')
  .then(response => response.json())
  .then(data => {
    regionAcronyms = data;
  })
  .catch(error => console.error('Error loading regions:', error));

function formatTwoDigits(value) {
  return value.length === 1 ? '0' + value : value;
}

function generateSensorID() {
  const region = document.getElementById('region').value;
  let cluster = document.getElementById('cluster').value;
  let number = document.getElementById('number').value;

  // Ensure cluster and number are two digits
  if (cluster) {
    cluster = formatTwoDigits(cluster);
  }
  if (number) {
    number = formatTwoDigits(number);
  }

  if (region && cluster && number) {
    const regionAcronym = regionAcronyms[region] || '';
    const sensorID = `${regionAcronym}-${cluster}-${number}`;
    document.getElementById('sensor_id').value = sensorID;
  }
}

// Attach event listeners to fields
document.getElementById('region').addEventListener('change', generateSensorID);
document.getElementById('cluster').addEventListener('input', generateSensorID);
document.getElementById('number').addEventListener('input', generateSensorID);

</script>

<!-- Modal Script End -->