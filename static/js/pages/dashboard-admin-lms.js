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
  const studentsCountByDayElement = document.getElementById('studentsCountByDayData');
  let studentsCountByDayOriginal = [];

  if (studentsCountByDayElement && studentsCountByDayElement.textContent) {
    const studentsCountByDayRaw = studentsCountByDayElement.textContent;
    studentsCountByDayOriginal = JSON.parse(studentsCountByDayRaw);
    console.log("studentsCountByDayOriginal", studentsCountByDayOriginal);
  } else {
    console.warn('Element with ID "studentsCountByDayData" not found or empty.');
    studentsCountByDayOriginal = [];
  }

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
const studentsCountChartContainer = document.querySelector("#admin-overall-students-chart");
if (studentsCountChartContainer) {
  const studentsCountChart = new ApexCharts(
    studentsCountChartContainer,
    studentsCountOptions
  );
  studentsCountChart.render();
} else {
  console.warn('Chart container "#admin-overall-students-chart" not found.');
}



// Total Courses
const coursesCountByDayElement = document.getElementById('coursesCountByDayData');
let coursesCountByDayOriginal = [];

if (coursesCountByDayElement && coursesCountByDayElement.textContent) {
  const coursesCountByDayRaw = coursesCountByDayElement.textContent;
  console.log(coursesCountByDayRaw);
  coursesCountByDayOriginal = JSON.parse(coursesCountByDayRaw);
  console.log(coursesCountByDayOriginal);
} else {
  console.warn('Element with ID "coursesCountByDayData" not found or empty.');
  coursesCountByDayOriginal = [];
}

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

const enrollChartContainer = document.querySelector("#admin-total-registration-chart");
if (enrollChartContainer) {
  var enrollChart = new ApexCharts(
    enrollChartContainer,
    enrollOptions
  );
  enrollChart.render();
} else {
  console.warn('Chart container "#admin-total-registration-chart" not found.');
}

// Total Courses
const contentsCountByDayElement = document.getElementById('contentsCountByDayData');
let contentsCountByDayOriginal = [];

if (contentsCountByDayElement && contentsCountByDayElement.textContent) {
  const contentsCountByDayRaw = contentsCountByDayElement.textContent;
  console.log(contentsCountByDayRaw);
  contentsCountByDayOriginal = JSON.parse(contentsCountByDayRaw);
  console.log(contentsCountByDayOriginal);
} else {
  console.warn('Element with ID "contentsCountByDayData" not found or empty.');
  contentsCountByDayOriginal = [];
}

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
const courseChartContainer = document.querySelector("#admin-total-course-chart");
if (courseChartContainer) {
  var courseChart = new ApexCharts(
    courseChartContainer,
    courseOptions
  );
  courseChart.render();
} else {
  console.warn('Chart container "#admin-total-course-chart" not found.');
}


// Active Students
const activeStudentsByDayElement = document.getElementById('activeStudentsData');
let activeStudentsByDayOriginal = [];

if (activeStudentsByDayElement && activeStudentsByDayElement.textContent) {
  const activeStudentsByDayRaw = activeStudentsByDayElement.textContent;
  console.log(activeStudentsByDayRaw);
  activeStudentsByDayOriginal = JSON.parse(activeStudentsByDayRaw);
  console.log(activeStudentsByDayOriginal);
} else {
  console.warn('Element with ID "activeStudentsData" not found or empty.');
  activeStudentsByDayOriginal = [];
}

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
const activeStudentsChartContainer = document.querySelector("#admin-active-students-chart");
if (activeStudentsChartContainer) {
  const activeStudentsChart = new ApexCharts(
    activeStudentsChartContainer,
    activeStudentsOptions
  );
  activeStudentsChart.render();
} else {
  console.warn('Chart container "#admin-active-students-chart" not found.');
}






// Daily Active Users and Daily activities Chart
 // Read the raw JSON from the hidden script blocks
 const dailyActiveUsersElement = document.getElementById('dailyActiveUsersData');
 const dailyActivitiesElement = document.getElementById('dailyActivitiesData');

 let dailyActiveUsersOriginal = [];
 let dailyActivitiesOriginal = [];

 //Parse into JavaScript objects/arrays
 if (dailyActiveUsersElement && dailyActiveUsersElement.textContent) {
   const dailyActiveUsersRaw = dailyActiveUsersElement.textContent;
   dailyActiveUsersOriginal = JSON.parse(dailyActiveUsersRaw);
 } else {
   console.warn('Element with ID "dailyActiveUsersData" not found or empty.');
   dailyActiveUsersOriginal = [];
 }

 if (dailyActivitiesElement && dailyActivitiesElement.textContent) {
   const dailyActivitiesRaw = dailyActivitiesElement.textContent;
   dailyActivitiesOriginal = JSON.parse(dailyActivitiesRaw);
 } else {
   console.warn('Element with ID "dailyActivitiesData" not found or empty.');
   dailyActivitiesOriginal = [];
 }

// Function to fill missing days for the object format (date and value property names)
function fillMissingDaysForObjects(data, dateProperty, valueProperty, days = 30) {
    // Create a map of existing dates
    const dateMap = new Map();
    data.forEach(item => {
        dateMap.set(item[dateProperty], item);
    });

    // Create an array for the last N days
    const result = [];
    const today = new Date();
    for (let i = days - 1; i >= 0; i--) {
        const date = new Date(today);
        date.setDate(today.getDate() - i);
        const dateStr = date.toISOString().split('T')[0];

        // Use existing item or create a new one with zeros
        let newItem;
        if (dateMap.has(dateStr)) {
            newItem = {...dateMap.get(dateStr)};
        } else {
            newItem = {
                [dateProperty]: dateStr
            };

            // If we're tracking a specific value property, set it to 0
            if (valueProperty) {
                newItem[valueProperty] = 0;
            } else {
                // For multi-value objects, set all expected properties to 0
                newItem.content_open = 0;
                newItem.marker = 0;
                newItem.memo = 0;
                newItem.hand_writing_memo = 0;
                newItem.bookmark = 0;
                newItem.quiz_attempts = 0;
                newItem.active_students = 0;
            }
        }

        result.push(newItem);
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

const dailyActiveUsersChartContainer = document.querySelector("#daily-active-users-chart");
if (dailyActiveUsersChartContainer) {
  const dailyActiveUsersChart = new ApexCharts(
    dailyActiveUsersChartContainer,
    dailyActiveUsersChartOptions
  );
  dailyActiveUsersChart.render();
} else {
  console.warn('Chart container "#daily-active-users-chart" not found.');
}



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

const dailyActivitiesChartContainer = document.querySelector("#daily-activities-chart");
if (dailyActivitiesChartContainer) {
  const dailyActivitiesChart = new ApexCharts(
    dailyActivitiesChartContainer,
    dailyActivitiesChartOptions
  );
  dailyActivitiesChart.render();
} else {
  console.warn('Chart container "#daily-activities-chart" not found.');
}




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
const categoryOneChartContainer = document.querySelector("#category-one");
if (categoryOneChartContainer) {
  const categoryOneChart = new ApexCharts(
    categoryOneChartContainer,
    catrgoryOneOptions
  );
  categoryOneChart.render();
} else {
  console.warn('Chart container "#category-one" not found.');
}

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
const categoryTwoChartContainer = document.querySelector("#category-two");
if (categoryTwoChartContainer) {
  const categoryTwoChart = new ApexCharts(
    categoryTwoChartContainer,
    catrgoryTwoOptions
  );
  categoryTwoChart.render();
} else {
  console.warn('Chart container "#category-two" not found.');
}

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
const categoryThreeChartContainer = document.querySelector("#category-three");
if (categoryThreeChartContainer) {
  const categoryThreeChart = new ApexCharts(
    categoryThreeChartContainer,
    catrgoryThreeOptions
  );
  categoryThreeChart.render();
} else {
  console.warn('Chart container "#category-three" not found.');
}

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
const categoryFourChartContainer = document.querySelector("#category-four");
if (categoryFourChartContainer) {
  const categoryFourChart = new ApexCharts(
    categoryFourChartContainer,
    catrgoryFourOptions
  );
  categoryFourChart.render();
} else {
  console.warn('Chart container "#category-four" not found.');
}

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
const categoryFiveChartContainer = document.querySelector("#category-five");
if (categoryFiveChartContainer) {
  const categoryFiveChart = new ApexCharts(
    categoryFiveChartContainer,
    catrgoryFiveOptions
  );
  categoryFiveChart.render();
} else {
  console.warn('Chart container "#category-five" not found.');
}



// Daily Active Students Chart
const dailyActiveStudentsElement = document.getElementById('dailyActiveStudentsData');
let dailyActiveStudentsOriginal = [];

if (dailyActiveStudentsElement && dailyActiveStudentsElement.textContent) {
  const dailyActiveStudentsRaw = dailyActiveStudentsElement.textContent;
  dailyActiveStudentsOriginal = JSON.parse(dailyActiveStudentsRaw);
} else {
  console.warn('Element with ID "dailyActiveStudentsData" not found or empty.');
  dailyActiveStudentsOriginal = [];
}

const dailyActiveStudentsOptions = {
  series: [
    {
      name: "Daily Active Students",
      data: dailyActiveStudentsOriginal.map((item) => item.count || 0),
    },
  ],
};

const dailyActiveStudentsChartContainer = document.querySelector("#daily-active-students-chart");
if (dailyActiveStudentsChartContainer) {
  const dailyActiveStudentsChart = new ApexCharts(
    dailyActiveStudentsChartContainer,
    dailyActiveStudentsOptions
  );
  dailyActiveStudentsChart.render();
} else {
  console.warn('Chart container "#daily-active-students-chart" not found.');
}





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

// Initialize Daily Activity Chart on Course Detail page
document.addEventListener('DOMContentLoaded', function() {
    const dailyActivityDataElement = document.getElementById('dailyActivityData');

    if (dailyActivityDataElement && dailyActivityDataElement.textContent) {
        try {
            // Parse the JSON data
            const dailyActivityData = JSON.parse(dailyActivityDataElement.textContent);

            // Fill missing days for the data
            const filledData = fillMissingDaysForObjects(dailyActivityData, 'date', null, 30);

            // Extract dates for x-axis
            const dates = filledData.map(item => item.date);

            // Create the stacked bar chart options
            const dailyActivityChartOptions = {
                chart: {
                    type: "bar",
                    height: 350,
                    stacked: true,
                    toolbar: {
                        show: true,
                        tools: {
                            download: true,
                            selection: true,
                            zoom: true,
                            zoomin: true,
                            zoomout: true,
                            pan: true
                        }
                    },
                    zoom: {
                        enabled: true
                    }
                },
                responsive: [{
                    breakpoint: 480,
                    options: {
                        legend: {
                            position: 'bottom',
                            offsetX: -10,
                            offsetY: 0
                        }
                    }
                }],
                plotOptions: {
                    bar: {
                        horizontal: false,
                        columnWidth: '60%',
                        endingShape: 'rounded'
                    },
                },
                dataLabels: {
                    enabled: false
                },
                series: [
                    {
                        name: "Active Students",
                        data: filledData.map(item => item.active_students || 0)
                    },
                    {
                        name: "Content Views",
                        data: filledData.map(item => item.content_open || 0)
                    },
                    {
                        name: "Markers",
                        data: filledData.map(item => item.marker || 0)
                    },
                    {
                        name: "Memos",
                        data: filledData.map(item => item.memo || 0)
                    },
                    {
                        name: "Handwriting Memos",
                        data: filledData.map(item => item.hand_writing_memo || 0)
                    },
                    {
                        name: "Bookmarks",
                        data: filledData.map(item => item.bookmark || 0)
                    },
                    {
                        name: "Quiz Attempts",
                        data: filledData.map(item => item.quiz_attempts || 0)
                    }
                ],
                xaxis: {
                    categories: dates,
                    type: 'datetime',
                    labels: {
                        rotate: -45,
                        rotateAlways: false
                    }
                },
                yaxis: {
                    title: {
                        text: "Activities"
                    },
                    labels: {
                        formatter: (val) => Math.round(val)
                    }
                },
                legend: {
                    position: 'top',
                    horizontalAlign: 'left',
                    offsetX: 40,
                    onItemClick: {
                        toggleDataSeries: true
                    }
                },
                fill: {
                    opacity: 1
                },
                colors: ['#5F71FA', '#76D466', '#FF4626', '#FFC107', '#9C27B0', '#4CAF50', '#26e9ff'],
                tooltip: {
                    y: {
                        formatter: (val) => val.toLocaleString()
                    }
                }
            };

            // Render the chart
            const chartContainer = document.querySelector("#daily-activity-chart");
            if (chartContainer) {
                const dailyActivityChart = new ApexCharts(
                    chartContainer,
                    dailyActivityChartOptions
                );
                dailyActivityChart.render();

                // Add a second line chart for active students if needed
                const activeStudentsOptions = {
                    chart: {
                        type: "line",
                        height: 350,
                        toolbar: {
                            show: false
                        }
                    },
                    series: [
                        {
                            name: "Active Students",
                            data: filledData.map(item => item.active_students || 0)
                        }
                    ],
                    stroke: {
                        curve: 'smooth',
                        width: 3
                    },
                    markers: {
                        size: 4
                    },
                    xaxis: {
                        categories: dates,
                        type: 'datetime',
                        labels: {
                            show: false
                        }
                    },
                    yaxis: {
                        opposite: true,
                        title: {
                            text: "Active Students"
                        }
                    },
                    colors: ['#4CAF50'],
                    tooltip: {
                        y: {
                            formatter: (val) => val.toLocaleString()
                        }
                    }
                };

                // If you want to show active students as a separate chart, uncomment this
                /*
                const activeStudentsChartContainer = document.querySelector("#active-students-chart");
                if (activeStudentsChartContainer) {
                    const activeStudentsChart = new ApexCharts(
                        activeStudentsChartContainer,
                        activeStudentsOptions
                    );
                    activeStudentsChart.render();
                }
                */
            } else {
                console.warn('Chart container "#daily-activity-chart" not found.');
            }
        } catch (error) {
            console.error('Error parsing daily activity data:', error);
        }
    } else {
        console.warn('Daily activity data element not found or empty.');
    }
});