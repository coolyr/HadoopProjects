function changeBG(o) {
	o.style.backgroundColor = "#5C75AA";
}

function changeBG2(o) {
	o.style.backgroundColor = "";
}

function checkFile() {
	var file = $("#myfile").val();
	if (file == "") {
		// window.alert("please choose a file!");
		$("#fileHint").css("display", "block");
		return false;
	} else {
		$("#fileHint").css("display", "none");
		return true;
	}
}
