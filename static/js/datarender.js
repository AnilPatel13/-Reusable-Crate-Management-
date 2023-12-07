onload = function(){
     var rows = document.getElementById('datatablesSimple').getElementsByTagName('tbody')[0].getElementsByTagName('tr');
     for (i = 0; i < rows.length; i++) {
     rows[i].onclick = function() {
     console.log(this.rowIndex);
     var ticket_no = this.cells[0].innerHTML;
     var emp_id = this.cells[1].innerHTML;
     var emp_name = this.cells[2].innerHTML;
     var priority = this.cells[3].innerHTML;
     var asset_model = this.cells[4].innerHTML;
     var asset_tag= this.cells[5].innerHTML;
     var asset_serial_no= this.cells[6].innerHTML;
     var no_of_unit = this.cells[7].innerHTML;
     var service_type = this.cells[8].innerHTML;
     var support_service = this.cells[9].innerHTML;
     var additional_comment = this.cells[10].innerHTML;
     var ticket_status = this.cells[11].innerHTML;
     var creation_date = this.cells[12].innerHTML;

     var request_fullfillment = {
        "ticket_no": ticket_no,
        "emp_id": emp_id,
        "emp_name": emp_name,
        "priority": priority,
        "asset_model": asset_model,
        "asset_tag": asset_tag,
        "asset_serial_no": asset_serial_no,
        "no_of_unit": no_of_unit,
        "service_type": service_type,
        "support_service": support_service,
        "additional_comment": additional_comment,
        "ticket_status": ticket_status,
        "creation_date": creation_date
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