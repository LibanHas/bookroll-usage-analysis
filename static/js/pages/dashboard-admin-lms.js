/**
 * Dashboard Admin LMS Javascript
 *
 * This file contains code for the admin LMS dashboard charts and data visualization.
 * It includes functions to handle data preparation for the charts, particularly to ensure
 * that days with no data are represented with zeros instead of being skipped.
 *
 * Two data filling functions are used:
 * 1. fillMissingDays - For array format data like [date, value]
 * 2. fillMissingDaysForObjects - For object format data like {date: '2023-01-01', value: 5}
 *
 * These functions create a complete dataset for the specified time range (default is last 7 days
 * for fillMissingDays and last 30 days for fillMissingDaysForObjects).
 */

// Total Students
  const studentsCountByDayRaw = document.getElementById('studentsCountByDayData').textContent;

  const studentsCountByDayOriginal = JSON.parse(studentsCountByDayRaw);
  console.log("studentsCountByDayOriginal", studentsCountByDayOriginal);

  // Apply the function to fill missing days
  const studentsCountByDay = fillMissingDays(studentsCountByDayOriginal);
  console.log("studentsCountByDay", studentsCountByDay);

  const studentsCountOptions = {
  series: [
    {
      name: "Students",
      data: studentsCountByDay.map((item) => item[1]),
    },
  ],
  chart: {
    type: "area",
    height: 70,
    zoom: {
      enabled: false,
    },
    sparkline: {
      enabled: true,
    },

    zoom: {
      autoScaleYaxis: true,
    },
    events: {
      mounted: (chart) => {
        chart.windowResizeHandler();
      },
    },
  },
  tooltip: {
    enabled: true,
    x: {
      show: false,
    },
    y: {
      title: {
        formatter: function (seriesName) {
          return "";
        },
      },
    },
    marker: {
      show: false,
    },
  },
  colors: ["#5F71FA"],
  stroke: {
    width: 1.2,
    curve: "straight",
  },
  fill: {
    type: "gradient",
    gradient: {
      opacityFrom: 0.5,
      opacityTo: 0.2,
      stops: [0, 60],
    },
  },
};
const studentsCountChart = new ApexCharts(
  document.querySelector("#admin-overall-students-chart"),
  studentsCountOptions
);
studentsCountChart.render();



// Total Courses
const coursesCountByDayRaw = document.getElementById('coursesCountByDayData').textContent;
console.log(coursesCountByDayRaw);
const coursesCountByDayOriginal = JSON.parse(coursesCountByDayRaw);
console.log(coursesCountByDayOriginal);

// Function to fill missing days with zeros for the last 7 days
function fillMissingDays(data) {
  // Create a map of existing dates
  const dateMap = new Map();
  data.forEach(item => {
    // Convert to date object and then format as YYYY-MM-DD
    const dateStr = new Date(item[0]).toISOString().split('T')[0];
    dateMap.set(dateStr, item[1]);
  });

  // Create an array for the last 7 days
  const result = [];
  const today = new Date();
  for (let i = 6; i >= 0; i--) {
    const date = new Date(today);
    date.setDate(today.getDate() - i);
    const dateStr = date.toISOString().split('T')[0];

    // Use existing count or 0 if no data for that day
    const count = dateMap.has(dateStr) ? dateMap.get(dateStr) : 0;
    result.push([dateStr, count]);
  }

  return result;
}

// Apply the function to fill missing days
const coursesCountByDay = fillMissingDays(coursesCountByDayOriginal);

var enrollOptions = {
  series: [
    {
      name: "Courses",
      data: coursesCountByDay.map((item) => item[1]),
    },
  ],
  chart: {
    type: "area",
    height: 70,
    zoom: {
      enabled: false,
    },
    sparkline: {
      enabled: true,
    },
    zoom: {
      autoScaleYaxis: true,
    },
    events: {
      mounted: (chart) => {
        chart.windowResizeHandler();
      },
    },
  },
  tooltip: {
    enabled: true,
    x: {
      show: false,
    },
    y: {
      title: {
        formatter: function (seriesName) {
          return "";
        },
      },
    },
    marker: {
      show: false,
    },
  },
  colors: ["#FF4626"],
  stroke: {
    width: 1.2,
    curve: "straight",
  },
  fill: {
    type: "gradient",
    gradient: {
      opacityFrom: 0.5,
      opacityTo: 0.2,
      stops: [0, 60],
    },
  },
};

var enrollChart = new ApexCharts(
  document.querySelector("#admin-total-registration-chart"),
  enrollOptions
);
enrollChart.render();

// Total Courses
const contentsCountByDayRaw = document.getElementById('contentsCountByDayData').textContent;
console.log(contentsCountByDayRaw);
const contentsCountByDayOriginal = JSON.parse(contentsCountByDayRaw);
console.log(contentsCountByDayOriginal);

// Apply the function to fill missing days
const contentsCountByDay = fillMissingDays(contentsCountByDayOriginal);

var courseOptions = {
  series: [
    {
      name: "Course",
      data: contentsCountByDay.map((item) => item[1]),
    },
  ],
  chart: {
    type: "area",
    height: 70,
    zoom: {
      enabled: false,
    },
    sparkline: {
      enabled: true,
    },

    zoom: {
      autoScaleYaxis: true,
    },
    events: {
      mounted: (chart) => {
        chart.windowResizeHandler();
      },
    },
  },
  tooltip: {
    enabled: true,
    x: {
      show: false,
    },
    y: {
      title: {
        formatter: function (seriesName) {
          return "";
        },
      },
    },
    marker: {
      show: false,
    },
  },
  colors: ["#5F71FA"],
  stroke: {
    width: 1.2,
    curve: "straight",
  },
  fill: {
    type: "gradient",
    gradient: {
      opacityFrom: 0.5,
      opacityTo: 0.2,
      stops: [0, 60],
    },
  },
};
var courseChart = new ApexCharts(
  document.querySelector("#admin-total-course-chart"),
  courseOptions
);
courseChart.render();


// Active Students
const activeStudentsByDayRaw = document.getElementById('activeStudentsData').textContent;
console.log(activeStudentsByDayRaw);
const activeStudentsByDayOriginal = JSON.parse(activeStudentsByDayRaw);
console.log(activeStudentsByDayOriginal);

// Apply the function to fill missing days
const activeStudentsByDay = fillMissingDays(activeStudentsByDayOriginal);

const activeStudentsOptions = {
  series: [
    {
      name: "Active Students",
      data: activeStudentsByDay.map((item) => item[1]),
    },
  ],
  chart: {
    type: "area",
    width: "100%",
    height: 70,
    zoom: {
      enabled: false,
    },
    sparkline: {
      enabled: true,
    },

    zoom: {
      autoScaleYaxis: true,
    },
    events: {
      mounted: (chart) => {
        chart.windowResizeHandler();
      },
    },
  },
  tooltip: {
    enabled: true,
    x: {
      show: false,
    },
    y: {
      title: {
        formatter: function (seriesName) {
          return "";
        },
      },
    },
    marker: {
      show: false,
    },
  },
  colors: ["#5F71FA"],
  stroke: {
    width: 1.2,
    curve: "straight",
  },
  fill: {
    type: "gradient",
    gradient: {
      opacityFrom: 0.5,
      opacityTo: 0.2,
      stops: [0, 60],
    },
  },
};
const activeStudentsChart = new ApexCharts(
  document.querySelector("#admin-active-students-chart"),
  activeStudentsOptions
);
activeStudentsChart.render();






// Daily Active Users and Daily activities Chart
 // Read the raw JSON from the hidden script blocks
 const dailyActiveUsersRaw = document.getElementById('dailyActiveUsersData').textContent;
 const dailyActivitiesRaw = document.getElementById('dailyActivitiesData').textContent;

 //Parse into JavaScript objects/arrays
 const dailyActiveUsersOriginal = JSON.parse(dailyActiveUsersRaw);
 const dailyActivitiesOriginal = JSON.parse(dailyActivitiesRaw);

 // Function to fill missing days for the object format (date and value property names)
 function fillMissingDaysForObjects(data, dateProperty, valueProperty, days = 30) {
   // Create a map of existing dates
   const dateMap = new Map();
   data.forEach(item => {
     dateMap.set(item[dateProperty], item[valueProperty]);
   });

   // Create an array for the last N days
   const result = [];
   const today = new Date();
   for (let i = days - 1; i >= 0; i--) {
     const date = new Date(today);
     date.setDate(today.getDate() - i);
     const dateStr = date.toISOString().split('T')[0];

     // Use existing count or 0 if no data for that day
     const count = dateMap.has(dateStr) ? dateMap.get(dateStr) : 0;

     const obj = {};
     obj[dateProperty] = dateStr;
     obj[valueProperty] = count;
     result.push(obj);
   }

   return result;
 }

 // Apply the function to fill missing days
 const dailyActiveUsers = fillMissingDaysForObjects(dailyActiveUsersOriginal, 'date', 'total_active_users');
 const dailyActivities = fillMissingDaysForObjects(dailyActivitiesOriginal, 'date', 'total_activities');

 // Extract data from dailyActiveUsers
 const datesActive = dailyActiveUsers.map((item) => item.date);
 const activeUsersData = dailyActiveUsers.map((item) => item.total_active_users);

 // Extract data from dailyActivities
 const datesActivities = dailyActivities.map((item) => item.date);
 const activitiesData = dailyActivities.map((item) => item.total_activities);


 const dailyActiveUsersChartOptions = {
  chart: {
    type: "bar",
    height: 320,
    width: "100%",
    offsetX: -5,
    offsetY: 15,
    zoom: {
      enabled: true, // Enables zooming
      type: 'x',     // Zoom along x-axis only
      autoScaleYaxis: true, // Auto scales Y-axis while zooming
    },
    toolbar: { show: true },
    events: {
      mounted: (chart) => {
        chart.windowResizeHandler();
      },
    },
  },
  colors: ['#76d466'],
  xaxis: {
    categories: datesActive, // Use the dates from dailyActiveUsers
    type: 'datetime',
  },
  yaxis: {
    labels: {
      formatter: (val) => val.toLocaleString(),
    },
  },
  series: [
    {
      name: "Daily Active Users",
      data: activeUsersData, // Only daily active users data
    },
  ],
  plotOptions: {
    bar: {
      horizontal: false,
      columnWidth: "18%",
      endingShape: "rounded",
    },
  },
  grid: {
    borderColor: "#EEE",
  },
  dataLabels: {
    enabled: false,
  },
  stroke: {
    curve: "smooth",
    width: 1,
  },
  legend: {
    position: "top",
    horizontalAlign: "left",
    offsetX: -30,
    offsetY: 0,
  },
  tooltip: {
    y: {
      formatter: (val) => val.toLocaleString(),
    },
  },
};

const dailyActiveUsersChart = new ApexCharts(
  document.querySelector("#daily-active-users-chart"),
  dailyActiveUsersChartOptions
);
dailyActiveUsersChart.render();



// Daily active users chart
const dailyActivitiesChartOptions = {
  chart: {
    type: "bar",
    height: 320,
    width: "100%",
    offsetX: -5,
    offsetY: 15,
    zoom: {
      enabled: true, // Enables zooming
      type: 'x',     // Zoom along x-axis only
      autoScaleYaxis: true, // Auto scales Y-axis while zooming
    },
    toolbar: { show: true },
    events: {
      mounted: (chart) => {
        chart.windowResizeHandler();
      },
    },
  },
  xaxis: {
    categories: datesActivities, // Use the dates from dailyActivities
    type: 'datetime',
  },
  yaxis: {
    labels: {
      formatter: (val) => val.toLocaleString(),
    },
  },
  series: [
    {
      name: "Daily Activities",
      data: activitiesData, // Only daily activities data
    },
  ],
  plotOptions: {
    bar: {
      horizontal: false,
      columnWidth: "18%",
      endingShape: "rounded",
    },
  },
  grid: {
    borderColor: "#EEE",
  },
  dataLabels: {
    enabled: false,
  },
  stroke: {
    curve: "smooth",
    width: 1,
  },
  legend: {
    position: "top",
    horizontalAlign: "left",
    offsetX: -30,
    offsetY: 0,
  },
  tooltip: {
    y: {
      formatter: (val) => val.toLocaleString(),
    },
  },
};

const dailyActivitiesChart = new ApexCharts(
  document.querySelector("#daily-activities-chart"),
  dailyActivitiesChartOptions
);
dailyActivitiesChart.render();




// Trending Categories  Chart
// Chart One (Graphic Design)
const catrgoryOneOptions = {
  series: [
    {
      name: "graphic",
      data: [5, 15, 10, 25, 28, 16, 18, 28, 30],
    },
  ],
  chart: {
    type: "area",
    height: 30,
    width: "80px",
    zoom: {
      enabled: false,
    },
    sparkline: {
      enabled: true,
    },
  },
  colors: ["#76D466"],
  stroke: {
    width: 1,
    curve: "straight",
  },
  tooltip: {
    enabled: true,
    x: {
      show: false,
    },
    y: {
      title: {
        formatter: function (seriesName) {
          return "";
        },
      },
    },
    marker: {
      show: false,
    },
  },
  fill: {
    type: "gradient",
    gradient: {
      opacityFrom: 0.5,
      opacityTo: 0.2,
      stops: [0, 60],
    },
  },
};
const categoryOneChart = new ApexCharts(
  document.querySelector("#category-one"),
  catrgoryOneOptions
);
categoryOneChart.render();

// Chart Two (UI/UX Design)
const catrgoryTwoOptions = {
  series: [
    {
      name: "ui/ux",
      data: [30, 28, 18, 16, 28, 25, 10, 15, 5],
    },
  ],
  chart: {
    type: "area",
    height: 30,
    width: "80px",
    zoom: {
      enabled: false,
    },
    sparkline: {
      enabled: true,
    },
  },
  colors: ["#FF4626"],
  stroke: {
    width: 1,
    curve: "straight",
  },
  tooltip: {
    enabled: true,
    x: {
      show: false,
    },
    y: {
      title: {
        formatter: function (seriesName) {
          return "";
        },
      },
    },
    marker: {
      show: false,
    },
  },
  fill: {
    type: "gradient",
    gradient: {
      opacityFrom: 0.5,
      opacityTo: 0.2,
      stops: [0, 60],
    },
  },
};
const categoryTwoChart = new ApexCharts(
  document.querySelector("#category-two"),
  catrgoryTwoOptions
);
categoryTwoChart.render();

// Chart Three (Web Development)
const catrgoryThreeOptions = {
  series: [
    {
      name: "web dev",
      data: [5, 15, 10, 25, 28, 16, 18, 28, 30],
    },
  ],
  chart: {
    type: "area",
    height: 30,
    width: "80px",
    zoom: {
      enabled: false,
    },
    sparkline: {
      enabled: true,
    },
  },
  colors: ["#76D466"],
  stroke: {
    width: 1,
    curve: "straight",
  },
  tooltip: {
    enabled: true,
    x: {
      show: false,
    },
    y: {
      title: {
        formatter: function (seriesName) {
          return "";
        },
      },
    },
    marker: {
      show: false,
    },
  },
  fill: {
    type: "gradient",
    gradient: {
      opacityFrom: 0.5,
      opacityTo: 0.2,
      stops: [0, 60],
    },
  },
};
const categoryThreeChart = new ApexCharts(
  document.querySelector("#category-three"),
  catrgoryThreeOptions
);
categoryThreeChart.render();

// Chart Four (Digital Marketing)
const catrgoryFourOptions = {
  series: [
    {
      name: "digital marketing",
      data: [5, 15, 10, 25, 28, 16, 18, 28, 30],
    },
  ],
  chart: {
    type: "area",
    height: 30,
    width: "80px",
    zoom: {
      enabled: false,
    },
    sparkline: {
      enabled: true,
    },
  },
  colors: ["#76D466"],
  stroke: {
    width: 1,
    curve: "straight",
  },
  tooltip: {
    enabled: true,
    x: {
      show: false,
    },
    y: {
      title: {
        formatter: function (seriesName) {
          return "";
        },
      },
    },
    marker: {
      show: false,
    },
  },
  fill: {
    type: "gradient",
    gradient: {
      opacityFrom: 0.5,
      opacityTo: 0.2,
      stops: [0, 60],
    },
  },
};
const categoryFourChart = new ApexCharts(
  document.querySelector("#category-four"),
  catrgoryFourOptions
);
categoryFourChart.render();

// Chart Five (Business Development)
const catrgoryFiveOptions = {
  series: [
    {
      name: "business dev",
      data: [30, 28, 18, 16, 28, 25, 10, 15, 5],
    },
  ],
  chart: {
    type: "area",
    height: 30,
    width: "80px",
    zoom: {
      enabled: false,
    },
    sparkline: {
      enabled: true,
    },
  },
  colors: ["#FF4626"],
  stroke: {
    width: 1,
    curve: "straight",
  },
  tooltip: {
    enabled: true,
    x: {
      show: false,
    },
    y: {
      title: {
        formatter: function (seriesName) {
          return "";
        },
      },
    },
    marker: {
      show: false,
    },
  },
  fill: {
    type: "gradient",
    gradient: {
      opacityFrom: 0.5,
      opacityTo: 0.2,
      stops: [0, 60],
    },
  },
};
const categoryFiveChart = new ApexCharts(
  document.querySelector("#category-five"),
  catrgoryFiveOptions
);
categoryFiveChart.render();
/*
// Learn Activity Chart
const learnActivityOptions = {
  series: [
    {
      name: "Paid course",
      data: [25, 15, 25, 10, 8],
    },
    {
      name: "Free course",
      data: [13, 6, 25, 3, 2],
    },
  ],
  chart: {
    type: "bar",
    height: "370",
    offsetX: -10,
    offsetY: 15,
    toolbar: {
      show: false,
    },
    events: {
      mounted: (chart) => {
        chart.windowResizeHandler();
      },
    },
  },
  plotOptions: {
    bar: {
      horizontal: false,
      columnWidth: "18%",
      endingShape: "rounded",
    },
  },
  dataLabels: {
    enabled: false,
  },
  grid: {
    borderColor: "#EEE",
  },
  stroke: {
    show: false,
  },
  xaxis: {
    categories: ["Design", "Marketing", "Business", "Web Dev", "Productivity"],
  },
  yaxis: {
    min: 0,
    max: 30,
    stepSize: 5,
    tickAmount: 6,
    labels: {
      formatter: (val) => val + "h",
    },
  },
  fill: {
    colors: ["#5F71FA", "#76D466"],
    opacity: 1,
  },
  legend: {
    position: "top",
    horizontalAlign: "left",
    offsetX: -30,
    markers: {
      width: 7,
      height: 7,
      radius: 99,
      fillColors: ["#5F71FA", "#76D466"],
      offsetX: -3,
      offsetY: -1,
    },
    itemMargin: {
      horizontal: 20,
    },
  },
  tooltip: {
    y: {
      formatter: (val) => {
        return val + "h";
      },
    },
  },
};

const learnActivity = new ApexCharts(
  document.querySelector("#admin-learn-activity-chart"),
  learnActivityOptions
);
learnActivity.render();
*/