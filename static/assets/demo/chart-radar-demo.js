// Set new default font family and font color to mimic Bootstrap's default styling
Chart.defaults.global.defaultFontFamily = '-apple-system,system-ui,BlinkMacSystemFont,"Segoe UI",Roboto,"Helvetica Neue",Arial,sans-serif';
Chart.defaults.global.defaultFontColor = '#292b2c';

// Pie Chart Example
var ctx = document.getElementById("myRadarChart");
var myRadarChart = new Chart(ctx, {
  type: 'radar',
  data: {
    labels: ["January", "February", "March", "April", "May", "June", "July","August","September","October","November","December"],
    datasets: [
    {
      label: 'Dataset 1',
      data: [4215, 5312, 6251, 7841, 9821, 14984,4215, 5312, 6251, 7841, 9821, 14984],
      backgroundColor: "rgba(255, 99, 132,0.5)",
      borderColor: "rgba(255, 99, 132)",
    },
    {
      label: 'Dataset 2',
      data: [3000, 1500, 1000, 14984, 1200,4000,3000, 1500, 1000, 14984, 1200,4000],
      backgroundColor: "rgba(54, 162, 235,0.5)",
      borderColor: "rgba(54, 162, 235)",
    }
  ]
  },
});
