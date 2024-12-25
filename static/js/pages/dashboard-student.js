// Get the raw data from the DOM
const dailystudentactivityRaw = document.getElementById('dailystudentactivity').textContent;

// Parse the raw data (assuming it's JSON)
const parsedData = JSON.parse(dailystudentactivityRaw);

// Transform the data for ApexCharts
const groupedData = parsedData.reduce((acc, row) => {
  const { date, operation_name, daily_count } = row;

  // Convert date to string if needed
  const dateString = typeof date === "string" ? date : date.toISOString().split('T')[0];

  if (!acc[dateString]) acc[dateString] = {};
  acc[dateString][operation_name] = daily_count;

  return acc;
}, {});

// Prepare categories (x-axis labels)
const categories = Object.keys(groupedData);

// Get unique operation names (stack names)
const operationNames = [...new Set(parsedData.map(row => row.operation_name))];

// Prepare series data
const series = operationNames.map(operationName => ({
  name: operationName,
  data: categories.map(date => groupedData[date][operationName] || 0), // Fill missing counts with 0
}));

// Stacked Bar Chart Options
const studentActivityOptions = {
  chart: {
    type: "bar",
    height: 400,
    width: "100%",
    stacked: true,
    zoom: {
      enabled: true, // Enables zooming
      type: 'x',     // Zoom along x-axis only
      autoScaleYaxis: true, // Auto scales Y-axis while zooming
    },
    toolbar: {
      show: true,
    },
    
  },
  xaxis: {
    categories: categories,
  },
  yaxis: {
    labels: {
      formatter: (val) => val,
    },
  },
  series: series,
  plotOptions: {
    bar: {
      horizontal: false,
      columnWidth: "50%",
    },
  },
  dataLabels: {
    enabled: false,
  },
  legend: {
    position: "top",
    horizontalAlign: "left",
  },
  tooltip: {
    y: {
      formatter: (val) => val,
    },
  },
};

// Render the chart
const studentActivityChart = new ApexCharts(
  document.querySelector("#student-average-learning-chart"),
  studentActivityOptions
);
studentActivityChart.render();
