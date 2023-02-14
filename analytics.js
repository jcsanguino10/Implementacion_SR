
// Get the Google Analytics cookie value
function getGACookie() {
  var cookieName = "_ga";
  var cookieValue = "";
  var cookies = document.cookie.split(";");
  for (var i = 0; i < cookies.length; i++) {
    var cookie = cookies[i].trim();
    if (cookie.indexOf(cookieName + "=") === 0) {
      cookieValue = cookie.substring(cookieName.length + 1, cookie.length);
      break;
    }
  }
  return cookieValue;
}

var gaCookie = getGACookie();
console.log("Google Analytics cookie value:", gaCookie);
