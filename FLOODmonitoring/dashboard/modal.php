<!-- New Rate Modal -->
<div id="AddDeviceModal" class="modal">
  <div class="modal-content modal-sm">
    <div class="modal-header">Adding New Device</div>
    <div class="modal-body">
      <div class="device-location">

      <form id="NEWdeviceFORM" enctype="multipart/form-data">
        <div>
          <label for="region">Region:</label>
          <select id="region" name="region" onchange="loadAreas()">
            <option value="">--Select a Region--</option>
            <option value="National Capital Region">National Capital Region</option>
            <option value="Ilocos Region">Ilocos Region</option>
            <option value="Cagayan Valley">Cagayan Valley</option>
            <option value="Central Luzon">Central Luzon</option>
            <option value="CALABARZON">CALABARZON</option>
            <option value="Cordillera Administrative Region">Cordillera Administrative Region</option>
            <option value="Bicol Region">Bicol Region</option>
            <option value="Western Visayas">Western Visayas</option>
            <option value="Central Visayas">Central Visayas</option>
            <option value="Eastern Visayas">Eastern Visayas</option>
            <option value="Zamboanga Peninsula">Zamboanga Peninsula</option>
            <option value="Northern Mindanao">Northern Mindanao</option>
            <option value="Davao Region">Davao Region</option>
            <option value="SOCCSKSARGEN">SOCCSKSARGEN</option>
            <option value="BARMM">BARMM</option>
            <option value="Caraga Region">Caraga Region</option>
            <option value="MIMAROPA">MIMAROPA</option>
          </select>
        </div>

        <div>
          <label for="area">Area:</label>
          <select id="area" name="area">
              <option value="">--Select an Area--</option>
              <!-- Areas will be dynamically loaded here -->
          </select>
        </div>

        <div>
          <label for="branch">Branch:</label>
          <input id="branch" name="branch" type="text" placeholder="Branch Name">
        </div>

        <div class="device-group">
          <div>
            <label for="cluster">Cluster:</label>
            <input id="cluster" name="cluster" type="text" id="cluster" name="cluster" placeholder="Cluster">
          </div>
          <div>
            <label for="number">Number:</label>
            <input id="number" name="number" type="text" placeholder="Number">
          </div>
        </div>
        
        <div class="device-group">
          <div>
            <label for="latitude">Latitude:</label>
            <input id="latitude" name="latitude" type="text" placeholder="Latitude">
          </div>
          <div>
            <label for="longitude">Longitude:</label>
            <input id="longitude" name="longitude" type="text" id="longitude" name="longitude" placeholder="Longitude">
          </div>
        </div>
        
        <div>
          <label for="sensor_id">Sensor ID:</label>
          <input id="sensor_id" name="sensor_id" type="text" placeholder="Sensor ID">
        </div>
      </form>

      </div>
    </div>
    <div class="modal-footer">
      <button class="btn-color-white" onclick="closeAddDeviceModal();">Cancel</button>
      <button class="btn-color-blue" onclick="confirmAddDeviceModal()">Confirm</button>
    </div>
  </div>
</div>

