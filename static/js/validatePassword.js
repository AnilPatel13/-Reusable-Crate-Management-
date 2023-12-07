function Validate() {
    var password = document.getElementById("inputPassword").value;
    var confirmPassword = document.getElementById("inputPasswordConfirm").value;
    if (password != confirmPassword) {
        alert("Passwords do not match.");
        return false;
    }
    return true;
}