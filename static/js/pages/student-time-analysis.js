/**
 * Student Time Analysis Charts
 *
 * This file handles the visualization of student activity data segregated by time categories:
 * - School Time: Activities during school hours on weekdays (excluding holidays)
 * - Non-School Time: Activities during weekends, holidays, and after/before school hours
 */

document.addEventListener('DOMContentLoaded', function() {
    // Get the time-categorized student data
    const studentTimeDataElement = document.getElementById('student-highlights-by-time-data');

    if (!studentTimeDataElement || !studentTimeDataElement.textContent) {
        console.warn('Time-categorized student data not found');
        return;
    }

    try {
        const studentTimeData = JSON.parse(studentTimeDataElement.textContent);

        // Global variables for chart management
        let currentTimeChart = null;
        let timeChartType = 'comparison'; // comparison, school_only, non_school_only
        let timePrivacyMode = true;

        // Get control elements
        const timeChartTypeSelect = document.getElementById('timeChartTypeSelect');
        const timePrivacyToggle = document.getElementById('timePrivacyToggle');

        // Initialize charts
        initializeTimeCharts();

        // Event listeners
        if (timeChartTypeSelect) {
            timeChartTypeSelect.addEventListener('change', function() {
                timeChartType = this.value;
                updateTimeChart();
            });
        }

        if (timePrivacyToggle) {
            timePrivacyToggle.addEventListener('click', function() {
                timePrivacyMode = !timePrivacyMode;
                updatePrivacyToggleButton();
                updateTimeChart();
            });
        }

        function initializeTimeCharts() {
            updateTimeChart();
            updatePrivacyToggleButton();
        }

        function updatePrivacyToggleButton() {
            if (timePrivacyToggle) {
                const icon = timePrivacyToggle.querySelector('i');
                const text = timePrivacyToggle.querySelector('span') || timePrivacyToggle;

                if (timePrivacyMode) {
                    icon.className = 'ri-eye-off-line mr-1';
                    text.textContent = text.textContent.replace('Show Names', 'Hide Names');
                } else {
                    icon.className = 'ri-eye-line mr-1';
                    text.textContent = text.textContent.replace('Hide Names', 'Show Names');
                }
            }
        }

        function anonymizeTimeData(data) {
            return data.map((student, index) => ({
                ...student,
                displayName: timePrivacyMode ? `Student ${String(index + 1).padStart(3, '0')}` : student.name
            }));
        }

        function updateTimeChart() {
            // Destroy existing chart
            if (currentTimeChart) {
                currentTimeChart.destroy();
                currentTimeChart = null;
            }

            // Filter out students with no activity
            const activeStudents = studentTimeData.filter(student => student.total_count > 0);

            switch (timeChartType) {
                case 'comparison':
                    renderComparisonChart(activeStudents);
                    break;
                case 'school_only':
                    renderSchoolTimeChart(activeStudents);
                    break;
                case 'non_school_only':
                    renderNonSchoolTimeChart(activeStudents);
                    break;
                case 'percentage':
                    renderPercentageChart(activeStudents);
                    break;
                case 'distribution':
                    renderTimeDistributionChart(activeStudents);
                    break;
            }
        }

        function renderComparisonChart(data) {
            const processedData = anonymizeTimeData(data);

            const categories = processedData.map(student => student.displayName);
            const schoolTimeData = processedData.map(student => student.school_time_count);
            const nonSchoolTimeData = processedData.map(student => student.non_school_time_count);

            const options = {
                series: [
                    {
                        name: 'School Time',
                        data: schoolTimeData,
                        color: '#5F71FA'
                    },
                    {
                        name: 'Non-School Time',
                        data: nonSchoolTimeData,
                        color: '#FF4626'
                    }
                ],
                chart: {
                    type: 'bar',
                    height: 500,
                    stacked: false,
                    toolbar: {
                        show: true
                    }
                },
                plotOptions: {
                    bar: {
                        horizontal: false,
                        columnWidth: '70%',
                        borderRadius: 4
                    }
                },
                dataLabels: {
                    enabled: false
                },
                xaxis: {
                    categories: categories,
                    labels: {
                        rotate: -45,
                        style: {
                            fontSize: '11px'
                        }
                    }
                },
                yaxis: {
                    title: {
                        text: 'Activity Count'
                    }
                },
                title: {
                    text: 'Student Activity: School Time vs Non-School Time',
                    align: 'center'
                },
                subtitle: {
                    text: 'Comparison of student activities during school hours and outside school hours',
                    align: 'center'
                },
                legend: {
                    position: 'top'
                },
                tooltip: {
                    shared: true,
                    intersect: false,
                    y: {
                        formatter: function(val) {
                            return val + ' activities';
                        }
                    }
                }
            };

            currentTimeChart = new ApexCharts(document.querySelector("#time-analysis-chart"), options);
            currentTimeChart.render();
        }

        function renderSchoolTimeChart(data) {
            const processedData = anonymizeTimeData(data);

            const categories = processedData.map(student => student.displayName);
            const schoolTimeData = processedData.map(student => student.school_time_count);

            const options = {
                series: [{
                    name: 'School Time Activities',
                    data: schoolTimeData,
                    color: '#5F71FA'
                }],
                chart: {
                    type: 'bar',
                    height: 500,
                    toolbar: {
                        show: true
                    }
                },
                plotOptions: {
                    bar: {
                        horizontal: false,
                        columnWidth: '60%',
                        borderRadius: 4
                    }
                },
                dataLabels: {
                    enabled: true,
                    offsetY: -20,
                    style: {
                        fontSize: '12px',
                        colors: ['#000']
                    }
                },
                xaxis: {
                    categories: categories,
                    labels: {
                        rotate: -45,
                        style: {
                            fontSize: '11px'
                        }
                    }
                },
                yaxis: {
                    title: {
                        text: 'Activity Count'
                    }
                },
                title: {
                    text: 'Student Activities During School Time',
                    align: 'center'
                },
                subtitle: {
                    text: 'Activities during school hours on weekdays (excluding holidays)',
                    align: 'center'
                },
                tooltip: {
                    y: {
                        formatter: function(val) {
                            return val + ' activities';
                        }
                    }
                }
            };

            currentTimeChart = new ApexCharts(document.querySelector("#time-analysis-chart"), options);
            currentTimeChart.render();
        }

        function renderNonSchoolTimeChart(data) {
            const processedData = anonymizeTimeData(data);

            const categories = processedData.map(student => student.displayName);
            const nonSchoolTimeData = processedData.map(student => student.non_school_time_count);

            const options = {
                series: [{
                    name: 'Non-School Time Activities',
                    data: nonSchoolTimeData,
                    color: '#FF4626'
                }],
                chart: {
                    type: 'bar',
                    height: 500,
                    toolbar: {
                        show: true
                    }
                },
                plotOptions: {
                    bar: {
                        horizontal: false,
                        columnWidth: '60%',
                        borderRadius: 4
                    }
                },
                dataLabels: {
                    enabled: true,
                    offsetY: -20,
                    style: {
                        fontSize: '12px',
                        colors: ['#000']
                    }
                },
                xaxis: {
                    categories: categories,
                    labels: {
                        rotate: -45,
                        style: {
                            fontSize: '11px'
                        }
                    }
                },
                yaxis: {
                    title: {
                        text: 'Activity Count'
                    }
                },
                title: {
                    text: 'Student Activities During Non-School Time',
                    align: 'center'
                },
                subtitle: {
                    text: 'Activities during weekends, holidays, and outside school hours',
                    align: 'center'
                },
                tooltip: {
                    y: {
                        formatter: function(val) {
                            return val + ' activities';
                        }
                    }
                }
            };

            currentTimeChart = new ApexCharts(document.querySelector("#time-analysis-chart"), options);
            currentTimeChart.render();
        }

        function renderPercentageChart(data) {
            const processedData = anonymizeTimeData(data);

            const categories = processedData.map(student => student.displayName);
            const schoolTimePercentages = processedData.map(student => student.school_time_percentage);
            const nonSchoolTimePercentages = processedData.map(student => student.non_school_time_percentage);

            const options = {
                series: [
                    {
                        name: 'School Time %',
                        data: schoolTimePercentages,
                        color: '#5F71FA'
                    },
                    {
                        name: 'Non-School Time %',
                        data: nonSchoolTimePercentages,
                        color: '#FF4626'
                    }
                ],
                chart: {
                    type: 'bar',
                    height: 500,
                    stacked: true,
                    stackType: '100%',
                    toolbar: {
                        show: true
                    }
                },
                plotOptions: {
                    bar: {
                        horizontal: false,
                        columnWidth: '70%',
                        borderRadius: 4
                    }
                },
                dataLabels: {
                    enabled: true,
                    formatter: function(val) {
                        return val.toFixed(1) + '%';
                    }
                },
                xaxis: {
                    categories: categories,
                    labels: {
                        rotate: -45,
                        style: {
                            fontSize: '11px'
                        }
                    }
                },
                yaxis: {
                    title: {
                        text: 'Percentage'
                    }
                },
                title: {
                    text: 'Student Activity Distribution by Time Category',
                    align: 'center'
                },
                subtitle: {
                    text: 'Percentage breakdown of activities during school vs non-school time',
                    align: 'center'
                },
                legend: {
                    position: 'top'
                },
                tooltip: {
                    y: {
                        formatter: function(val) {
                            return val.toFixed(1) + '%';
                        }
                    }
                }
            };

            currentTimeChart = new ApexCharts(document.querySelector("#time-analysis-chart"), options);
            currentTimeChart.render();
        }

        function renderTimeDistributionChart(data) {
            // Create distribution data for school time vs non-school time preferences
            const schoolTimePreferred = data.filter(student =>
                student.school_time_count > student.non_school_time_count && student.total_count > 0
            ).length;

            const nonSchoolTimePreferred = data.filter(student =>
                student.non_school_time_count > student.school_time_count && student.total_count > 0
            ).length;

            const equalUsage = data.filter(student =>
                student.school_time_count === student.non_school_time_count && student.total_count > 0
            ).length;

            const options = {
                series: [schoolTimePreferred, nonSchoolTimePreferred, equalUsage],
                chart: {
                    type: 'donut',
                    height: 400
                },
                labels: ['Prefer School Time', 'Prefer Non-School Time', 'Equal Usage'],
                colors: ['#5F71FA', '#FF4626', '#FFC107'],
                title: {
                    text: 'Student Time Preference Distribution',
                    align: 'center'
                },
                subtitle: {
                    text: 'Based on where students have more activity',
                    align: 'center'
                },
                legend: {
                    position: 'bottom'
                },
                tooltip: {
                    y: {
                        formatter: function(val) {
                            return val + ' students';
                        }
                    }
                },
                plotOptions: {
                    pie: {
                        donut: {
                            size: '70%',
                            labels: {
                                show: true,
                                total: {
                                    show: true,
                                    label: 'Total Students',
                                    formatter: function() {
                                        return data.length;
                                    }
                                }
                            }
                        }
                    }
                }
            };

            currentTimeChart = new ApexCharts(document.querySelector("#time-analysis-chart"), options);
            currentTimeChart.render();
        }

        // Generate summary statistics
        function generateTimeSummary() {
            const activeStudents = studentTimeData.filter(student => student.total_count > 0);

            if (activeStudents.length === 0) {
                return '<p class="text-gray-500">No active students found for the selected period.</p>';
            }

            const totalSchoolTime = activeStudents.reduce((sum, student) => sum + student.school_time_count, 0);
            const totalNonSchoolTime = activeStudents.reduce((sum, student) => sum + student.non_school_time_count, 0);
            const totalActivities = totalSchoolTime + totalNonSchoolTime;

            const schoolTimePercentage = totalActivities > 0 ? ((totalSchoolTime / totalActivities) * 100).toFixed(1) : 0;
            const nonSchoolTimePercentage = totalActivities > 0 ? ((totalNonSchoolTime / totalActivities) * 100).toFixed(1) : 0;

            const avgSchoolTime = (totalSchoolTime / activeStudents.length).toFixed(1);
            const avgNonSchoolTime = (totalNonSchoolTime / activeStudents.length).toFixed(1);

            return `
                <div class="grid grid-cols-2 md:grid-cols-4 gap-4 mb-4">
                    <div class="bg-blue-50 dark:bg-blue-900/20 p-3 rounded-lg text-center">
                        <div class="text-2xl font-bold text-blue-600 dark:text-blue-400">${totalSchoolTime}</div>
                        <div class="text-sm text-blue-800 dark:text-blue-300">School Time Activities</div>
                        <div class="text-xs text-blue-600 dark:text-blue-400">${schoolTimePercentage}% of total</div>
                    </div>
                    <div class="bg-red-50 dark:bg-red-900/20 p-3 rounded-lg text-center">
                        <div class="text-2xl font-bold text-red-600 dark:text-red-400">${totalNonSchoolTime}</div>
                        <div class="text-sm text-red-800 dark:text-red-300">Non-School Time Activities</div>
                        <div class="text-xs text-red-600 dark:text-red-400">${nonSchoolTimePercentage}% of total</div>
                    </div>
                    <div class="bg-green-50 dark:bg-green-900/20 p-3 rounded-lg text-center">
                        <div class="text-2xl font-bold text-green-600 dark:text-green-400">${avgSchoolTime}</div>
                        <div class="text-sm text-green-800 dark:text-green-300">Avg School Time per Student</div>
                    </div>
                    <div class="bg-yellow-50 dark:bg-yellow-900/20 p-3 rounded-lg text-center">
                        <div class="text-2xl font-bold text-yellow-600 dark:text-yellow-400">${avgNonSchoolTime}</div>
                        <div class="text-sm text-yellow-800 dark:text-yellow-300">Avg Non-School Time per Student</div>
                    </div>
                </div>
            `;
        }

        // Update summary
        const summaryContainer = document.getElementById('time-analysis-summary');
        if (summaryContainer) {
            summaryContainer.innerHTML = generateTimeSummary();
        }

    } catch (error) {
        console.error('Error parsing time-categorized student data:', error);
    }
});