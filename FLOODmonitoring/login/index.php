<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Document</title>

  <link rel="stylesheet" href="../css/montserrat.css">
  <link rel="stylesheet" href="../css/icon.css">
  <link rel="stylesheet" href="../css/element.css">
  <link rel="stylesheet" href="../css/style_login.css">
</head>
<body>
  <div class="container-center">
    <div class="login-card">
      <div class="login-title">
        Real-Time Flood Monitoring
      </div>
      <div class="login-form">
        <form id="loginform" enctype="multipart/form-data">
          <div class="login-field">
            <label for="username">Username</label>
            <input autocomplete="false" type="text" name="username" id="username" required>
            <label class="mt-5" for="password">Password</label>
            <input type="password" name="password" id="password" required>
            <button id="login_button" class="mt-10">Login</button>
          </div>
        </form>
      </div>
    </div>
  </div>
  
<?php include("../includes/admin_modal.php"); ?>

</body>

<script src="../js/jquery.min.js"></script>

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
</script>

<script>
  $("#login_button").click(function(e){
    e.preventDefault();
    console.log("Logging In");
    var form = $('#loginform')[0];
    var data = new FormData(form);
    $.ajax({
      type: "POST",
      enctype: 'multipart/form-data',
      url:"authenticate.php",
      data:data,
      processData: false,
      contentType: false,
      cache: false,
      success:function(data){
        json = JSON.parse(data);
        loginState = json.login;
        if(loginState == "SUCCESS"){
          window.location.href = "../dashboard";
        }else if(loginState == "FAIL"){
          $('#promptError').text("Login Failed!");
          $('#promptErrorSM').text("Wrong username or password.");
          openErrorModal();
        }else if(loginState == "NO USER"){
          $('#promptError').text("Login Failed!");
          $('#promptErrorSM').text("Wrong username or password.");
          openErrorModal();
        }else if(loginState == "EMPTY FIELD"){
          $('#promptError').text("Login Failed!");
          $('#promptErrorSM').text("Field Name cannot be empty.");
          openErrorModal();
        }
      }
    })
    return false;
  });
</script>
</html>