onload = function(){
     var rows = document.getElementById('datatablesSimple').getElementsByTagName('tbody')[0].getElementsByTagName('tr');
     for (i = 0; i < rows.length; i++) {
     rows[i].onclick = function() {
     console.log(this.rowIndex);
     var ticket_no = this.cells[0].innerHTML;


     var request_fullfillment = {
        "ticket_no": ticket_no
    }

     console.log(request_fullfillment);

     window.localStorage.setItem("request_fullfillment",JSON.stringify(request_fullfillment));
     console.log("retrieve records");
     var records = window.localStorage.getItem("request_fullfillment");
     var obj = JSON.parse(records);
     console.log(obj.ticket_no);
     }
}
}