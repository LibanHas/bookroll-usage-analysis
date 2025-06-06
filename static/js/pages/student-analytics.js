document.addEventListener('DOMContentLoaded', function() {
    console.log('Student Analytics Dashboard initializing...');

    // Time frame selector event handler
    const timeFrameSelect = document.getElementById('time-frame-select');
    if (timeFrameSelect) {
        timeFrameSelect.addEventListener('change', function() {
            const selectedTimeFrame = this.value;
            console.log('Time frame changed to:', selectedTimeFrame);

            // Show loading indicator
            const loadingIndicator = document.createElement('div');
            loadingIndicator.className = 'fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50';
            loadingIndicator.innerHTML = `
                <div class="bg-white dark:bg-gray-800 rounded-lg p-6 flex items-center gap-3">
                    <div class="animate-spin rounded-full h-6 w-6 border-b-2 border-primary-500"></div>
                    <span class="text-gray-700 dark:text-gray-300">Loading analytics...</span>
                </div>
            `;
            document.body.appendChild(loadingIndicator);

            // Reload the page with the new time frame parameter
            const url = new URL(window.location);
            url.searchParams.set('time_frame', selectedTimeFrame);
            window.location.href = url.toString();
        });
    }

    // Initialize default data structures
    let analyticsData = {
        overall_stats: {
            total_students: 0,
            total_activities: 0,
            avg_activities: 0,
            std_dev_activities: 0
        },
        activity_distribution: [],
        top_operations: [],
        daily_trends: []
    };

    let learningInsightsData = {
        engagement_levels: [],
        content_interactions: []
    };

    let hourlyHeatmapData = {
        series: [],
        stats: {
            max_activity: 0,
            min_activity: 0,
            avg_activity: 0
        }
    };

    // Parse data from hidden script tags with comprehensive error handling
    try {
        const analyticsElement = document.getElementById('analytics-data');
        if (analyticsElement && analyticsElement.textContent.trim()) {
            const parsedData = JSON.parse(analyticsElement.textContent);
            if (parsedData && typeof parsedData === 'object') {
                analyticsData = { ...analyticsData, ...parsedData };
                console.log('Analytics data loaded:', analyticsData);
            }
        }
    } catch (e) {
        console.warn('Failed to parse analytics data:', e);
    }

    try {
        const insightsElement = document.getElementById('learning-insights-data');
        if (insightsElement && insightsElement.textContent.trim()) {
            const parsedData = JSON.parse(insightsElement.textContent);
            if (parsedData && typeof parsedData === 'object') {
                learningInsightsData = { ...learningInsightsData, ...parsedData };
                console.log('Learning insights data loaded:', learningInsightsData);
            }
        }
    } catch (e) {
        console.warn('Failed to parse learning insights data:', e);
    }

    try {
        const heatmapElement = document.getElementById('hourly-heatmap-data');
        if (heatmapElement && heatmapElement.textContent.trim()) {
            const parsedData = JSON.parse(heatmapElement.textContent);
            if (parsedData && typeof parsedData === 'object') {
                hourlyHeatmapData = { ...hourlyHeatmapData, ...parsedData };
                console.log('Hourly heatmap data loaded:', hourlyHeatmapData);
            }
        }
    } catch (e) {
        console.warn('Failed to parse hourly heatmap data:', e);
    }

    // Parse time spent distribution data
    let timeSpentDistributionData = {
        statistics: {
            mean: 0,
            std_dev: 0,
            median: 0,
            mode: 0,
            min: 0,
            max: 0,
            count: 0
        },
        bins: [],
        normal_curve: []
    };

    try {
        const timeSpentElement = document.getElementById('time-spent-distribution-data');
        if (timeSpentElement && timeSpentElement.textContent.trim()) {
            const parsedData = JSON.parse(timeSpentElement.textContent);
            if (parsedData && typeof parsedData === 'object') {
                timeSpentDistributionData = { ...timeSpentDistributionData, ...parsedData };
                console.log('Time spent distribution data loaded:', timeSpentDistributionData);
            }
        }
    } catch (e) {
        console.warn('Failed to parse time spent distribution data:', e);
    }

    // Helper function to safely render charts
    function renderChart(elementId, options, fallbackMessage) {
        try {
            const element = document.querySelector(elementId);
            if (!element) {
                console.error(`Element ${elementId} not found`);
                return;
            }

            // Clear any existing content
            element.innerHTML = '';

            const chart = new ApexCharts(element, options);
            chart.render().then(() => {
                console.log(`Chart ${elementId} rendered successfully`);
            }).catch((error) => {
                console.error(`Error rendering chart ${elementId}:`, error);
                element.innerHTML = `<div class="flex items-center justify-center h-80 text-red-500">${fallbackMessage}</div>`;
            });
        } catch (error) {
            console.error(`Error initializing chart ${elementId}:`, error);
            const element = document.querySelector(elementId);
            if (element) {
                element.innerHTML = `<div class="flex items-center justify-center h-80 text-red-500">${fallbackMessage}</div>`;
            }
        }
    }

    // Activity Distribution Chart
    if (analyticsData.activity_distribution && analyticsData.activity_distribution.length > 0) {
        const activityDistributionOptions = {
            series: [{
                name: 'Students',
                data: analyticsData.activity_distribution.map(item => ({
                    x: item.range || 'Unknown',
                    y: item.count || 0
                }))
            }],
            chart: {
                type: 'bar',
                height: 320,
                toolbar: { show: false },
                background: 'transparent'
            },
            plotOptions: {
                bar: {
                    borderRadius: 4,
                    horizontal: false,
                    columnWidth: '60%'
                }
            },
            dataLabels: {
                enabled: true,
                style: {
                    fontSize: '12px'
                }
            },
            xaxis: {
                title: {
                    text: 'Activity Range',
                    style: { fontSize: '14px' }
                },
                labels: {
                    style: { fontSize: '12px' }
                }
            },
            yaxis: {
                title: {
                    text: 'Number of Students',
                    style: { fontSize: '14px' }
                },
                labels: {
                    style: { fontSize: '12px' }
                }
            },
            colors: ['#3B82F6'],
            title: {
                text: 'Student Distribution by Activity Level',
                align: 'left',
                style: { fontSize: '16px', fontWeight: 'bold' }
            },
            grid: {
                borderColor: '#e7e7e7',
                strokeDashArray: 3
            }
        };
        renderChart("#activity-distribution-chart", activityDistributionOptions, "Error loading activity distribution chart");
    } else {
        document.querySelector("#activity-distribution-chart").innerHTML =
            '<div class="flex items-center justify-center h-80 text-gray-500"><div class="text-center"><i class="ri-bar-chart-line text-4xl mb-2"></i><p>No activity distribution data available</p></div></div>';
    }

    // Engagement Levels Chart
    if (learningInsightsData.engagement_levels && learningInsightsData.engagement_levels.length > 0) {
        const engagementLevelsOptions = {
            series: learningInsightsData.engagement_levels.map(item => item.student_count || 0),
            chart: {
                type: 'donut',
                height: 320,
                background: 'transparent'
            },
            labels: learningInsightsData.engagement_levels.map(item => item.level || 'Unknown'),
            colors: ['#10B981', '#3B82F6', '#F59E0B', '#EF4444'],
            legend: {
                position: 'bottom',
                fontSize: '12px'
            },
            title: {
                text: 'Student Engagement Distribution',
                align: 'left',
                style: { fontSize: '16px', fontWeight: 'bold' }
            },
            dataLabels: {
                enabled: true,
                style: { fontSize: '12px' }
            },
            plotOptions: {
                pie: {
                    donut: {
                        size: '60%'
                    }
                }
            }
        };
        renderChart("#engagement-levels-chart", engagementLevelsOptions, "Error loading engagement levels chart");
    } else {
        document.querySelector("#engagement-levels-chart").innerHTML =
            '<div class="flex items-center justify-center h-80 text-gray-500"><div class="text-center"><i class="ri-pie-chart-line text-4xl mb-2"></i><p>No engagement data available</p></div></div>';
    }

    // Time Spent Distribution Chart (Normal Distribution)
    if (timeSpentDistributionData.bins && timeSpentDistributionData.bins.length > 0 && timeSpentDistributionData.statistics.count > 0) {
        const histogramSeries = [{
            name: 'Frequency',
            type: 'column',
            data: timeSpentDistributionData.bins.map(bin => ({
                x: bin.bin_center,
                y: bin.frequency
            }))
        }];

        // Add normal curve if available
        if (timeSpentDistributionData.normal_curve && timeSpentDistributionData.normal_curve.length > 0) {
            histogramSeries.push({
                name: 'Normal Distribution',
                type: 'line',
                data: timeSpentDistributionData.normal_curve.map(point => ({
                    x: point.x,
                    y: point.y * timeSpentDistributionData.statistics.count * (timeSpentDistributionData.bins[1] ? timeSpentDistributionData.bins[1].bin_center - timeSpentDistributionData.bins[0].bin_center : 1)
                }))
            });
        }

        const timeSpentDistributionOptions = {
            series: histogramSeries,
            chart: {
                type: 'line',
                height: 380,
                toolbar: { show: true },
                background: 'transparent'
            },
            stroke: {
                width: [0, 3],
                curve: 'smooth'
            },
            plotOptions: {
                bar: {
                    borderRadius: 2,
                    columnWidth: '80%',
                    dataLabels: {
                        position: 'top'
                    }
                }
            },
            dataLabels: {
                enabled: true,
                enabledOnSeries: [0],
                style: {
                    fontSize: '10px',
                    colors: ['#304758']
                },
                offsetY: -20,
                formatter: function(val) {
                    return val > 0 ? val : '';
                }
            },
            colors: ['#3B82F6', '#EF4444'],
            title: {
                text: `Daily Time Spent Distribution (μ=${timeSpentDistributionData.statistics.mean}h, σ=${timeSpentDistributionData.statistics.std_dev}h)`,
                align: 'left',
                style: { fontSize: '16px', fontWeight: 'bold' }
            },
            subtitle: {
                text: `n=${timeSpentDistributionData.statistics.count} data points | Median: ${timeSpentDistributionData.statistics.median}h | Mode: ${timeSpentDistributionData.statistics.mode}h | Max session: ${timeSpentDistributionData.statistics.max_session_hours || 1.5}h`,
                align: 'left',
                style: { fontSize: '12px', color: '#666' }
            },
            xaxis: {
                title: {
                    text: 'Daily Hours Spent',
                    style: { fontSize: '14px' }
                },
                labels: {
                    style: { fontSize: '12px' },
                    formatter: function(val) {
                        return parseFloat(val).toFixed(1) + 'h';
                    }
                },
                type: 'numeric',
                decimalsInFloat: 1
            },
            yaxis: [
                {
                    title: {
                        text: 'Frequency',
                        style: { fontSize: '14px' }
                    },
                    labels: {
                        style: { fontSize: '12px' },
                        formatter: function(val) {
                            return Math.round(val);
                        }
                    }
                },
                {
                    opposite: true,
                    title: {
                        text: 'Probability Density',
                        style: { fontSize: '14px' }
                    },
                    labels: {
                        style: { fontSize: '12px' },
                        formatter: function(val) {
                            return val.toFixed(3);
                        }
                    }
                }
            ],
            legend: {
                position: 'top',
                horizontalAlign: 'right',
                fontSize: '12px'
            },
            grid: {
                borderColor: '#e7e7e7',
                strokeDashArray: 3
            },
            tooltip: {
                shared: true,
                intersect: false,
                y: {
                    formatter: function(val, { seriesIndex, dataPointIndex }) {
                        if (seriesIndex === 0) {
                            // Histogram tooltip
                            const bin = timeSpentDistributionData.bins[dataPointIndex];
                            if (bin) {
                                return `<strong>${val} students</strong><br/>
                                       Range: ${bin.bin_start}h - ${bin.bin_end}h<br/>
                                       Percentage: ${((val / timeSpentDistributionData.statistics.count) * 100).toFixed(1)}%`;
                            }
                            return val + ' students';
                        } else {
                            // Normal curve tooltip
                            return 'Density: ' + val.toFixed(4);
                        }
                    }
                }
            },
            annotations: {
                xaxis: [
                    {
                        x: timeSpentDistributionData.statistics.mean,
                        borderColor: '#00E396',
                        label: {
                            text: `Mean: ${timeSpentDistributionData.statistics.mean}h`,
                            style: {
                                color: '#fff',
                                background: '#00E396'
                            }
                        }
                    },
                    {
                        x: timeSpentDistributionData.statistics.median,
                        borderColor: '#FF4560',
                        label: {
                            text: `Median: ${timeSpentDistributionData.statistics.median}h`,
                            style: {
                                color: '#fff',
                                background: '#FF4560'
                            }
                        }
                    }
                ]
            }
        };

        renderChart("#time-spent-distribution-chart", timeSpentDistributionOptions, "Error loading time spent distribution chart");
    } else {
        document.querySelector("#time-spent-distribution-chart").innerHTML =
            '<div class="flex items-center justify-center h-96 text-gray-500"><div class="text-center"><i class="ri-bar-chart-box-line text-4xl mb-2"></i><p>No time spent distribution data available</p><p class="text-sm mt-1">Data points: ' + (timeSpentDistributionData.statistics.count || 0) + '</p></div></div>';
    }

    // Hourly Activity Heatmap Chart
    if (hourlyHeatmapData.combined_series && hourlyHeatmapData.combined_series.length > 0) {

        // Calculate max values for color scaling
        const maxSchoolActivity = hourlyHeatmapData.stats.max_school_activity || 0;
        const maxNonSchoolActivity = hourlyHeatmapData.stats.max_non_school_activity || 0;

        // Global helper function to apply colors consistently
        window.applyHeatmapColors = function() {
            setTimeout(() => {
                const heatmapElements = document.querySelectorAll('#combined-activity-heatmap .apexcharts-heatmap-rect');

                heatmapElements.forEach((element, index) => {
                    try {
                        const seriesIndex = Math.floor(index / hourlyHeatmapData.combined_series[0].data.length);
                        const dataPointIndex = index % hourlyHeatmapData.combined_series[0].data.length;

                        if (hourlyHeatmapData.combined_series[seriesIndex] &&
                            hourlyHeatmapData.combined_series[seriesIndex].data[dataPointIndex]) {

                            const dataPoint = hourlyHeatmapData.combined_series[seriesIndex].data[dataPointIndex];
                            const isSchoolTime = dataPoint.school_time;
                            const activityValue = dataPoint.y;

                            if (activityValue > 0) {
                                let color;
                                if (isSchoolTime) {
                                    if (activityValue <= Math.floor(maxSchoolActivity * 0.25)) {
                                        color = '#C6E48B';
                                    } else if (activityValue <= Math.floor(maxSchoolActivity * 0.5)) {
                                        color = '#7BC96F';
                                    } else if (activityValue <= Math.floor(maxSchoolActivity * 0.75)) {
                                        color = '#239A3B';
                                    } else {
                                        color = '#196127';
                                    }
                                } else {
                                    if (activityValue <= Math.floor(maxNonSchoolActivity * 0.25)) {
                                        color = '#FED7AA';
                                    } else if (activityValue <= Math.floor(maxNonSchoolActivity * 0.5)) {
                                        color = '#FDBA74';
                                    } else if (activityValue <= Math.floor(maxNonSchoolActivity * 0.75)) {
                                        color = '#FB923C';
                                    } else {
                                        color = '#EA580C';
                                    }
                                }
                                element.setAttribute('fill', color);
                            }
                        }
                    } catch (error) {
                        console.warn('Error applying color to heatmap element:', error);
                    }
                });
            }, 100);
        };

        // Calculate engagement thresholds based on actual data
        function calculateEngagementThresholds(heatmapData) {
            const activitiesPerStudentValues = [];

            // Extract all activities per student values from the heatmap data
            if (heatmapData.combined_series) {
                heatmapData.combined_series.forEach(hourSeries => {
                    hourSeries.data.forEach(dataPoint => {
                        if (dataPoint.student_count > 0 && dataPoint.y > 0) {
                            const activitiesPerStudent = dataPoint.y / dataPoint.student_count;
                            activitiesPerStudentValues.push(activitiesPerStudent);
                        }
                    });
                });
            }

            if (activitiesPerStudentValues.length === 0) {
                // Fallback to default values if no data
                return {
                    high: 10,
                    moderate: 5,
                    light: 1,
                    brief: 0.1
                };
            }

            // Sort values to calculate percentiles
            const sortedValues = activitiesPerStudentValues.sort((a, b) => a - b);
            const maxValue = sortedValues[sortedValues.length - 1];
            const minValue = sortedValues[0];

            // Calculate thresholds using quartiles (more mathematically sound)
            const q1Index = Math.floor(sortedValues.length * 0.25);
            const q2Index = Math.floor(sortedValues.length * 0.50); // Median
            const q3Index = Math.floor(sortedValues.length * 0.75);

            const q1 = sortedValues[q1Index];
            const q2 = sortedValues[q2Index];
            const q3 = sortedValues[q3Index];

            // Alternative approach: Equal mathematical divisions of the range
            // This approach divides the min-max range into equal segments
            /*
            const range = maxValue - minValue;
            const step = range / 4;
            return {
                high: minValue + (step * 3),      // 75% of range
                moderate: minValue + (step * 2),  // 50% of range
                light: minValue + step,           // 25% of range
                brief: minValue + (step * 0.1)   // 2.5% of range
            };
            */

            console.log('Engagement threshold calculation:', {
                dataPoints: activitiesPerStudentValues.length,
                min: minValue.toFixed(2),
                max: maxValue.toFixed(2),
                q1: q1.toFixed(2),
                q2: q2.toFixed(2),
                q3: q3.toFixed(2),
                method: 'quartiles'
            });

            return {
                high: q3,        // Top 25% - High engagement
                moderate: q2,    // Above median - Moderate engagement
                light: q1,       // Above bottom 25% - Light engagement
                brief: Math.min(q1 / 2, minValue > 0 ? minValue : 0.1)  // Half of Q1 or minimal activity
            };
        }

        // Calculate thresholds for this dataset
        engagementThresholds = calculateEngagementThresholds(hourlyHeatmapData);
        console.log('Calculated engagement thresholds:', engagementThresholds);

        // Update the legend display with calculated thresholds
        function updateEngagementThresholdsDisplay() {
            if (engagementThresholds) {
                const display = document.getElementById('engagement-thresholds-display');
                if (display) {
                    document.getElementById('threshold-high').textContent = engagementThresholds.high.toFixed(1);
                    document.getElementById('threshold-moderate').textContent = engagementThresholds.moderate.toFixed(1);
                    document.getElementById('threshold-light').textContent = engagementThresholds.light.toFixed(1);
                    document.getElementById('threshold-brief').textContent = engagementThresholds.brief.toFixed(1);
                    display.classList.remove('hidden');
                }
            }
        }

        updateEngagementThresholdsDisplay();

        combinedHeatmapOptions = {
            series: hourlyHeatmapData.combined_series,
            chart: {
                height: 600, // Taller since it's a single comprehensive chart
                type: 'heatmap',
                toolbar: { show: false },
                background: 'transparent'
            },
            dataLabels: {
                enabled: false
            },
            title: {
                text: 'Student Activity Patterns by Time Type',
                align: 'left',
                style: { fontSize: '16px', fontWeight: 'bold' }
            },
            subtitle: {
                text: 'School Time (Green) vs Non-School Time (Orange) - JST timezone',
                align: 'left',
                style: { fontSize: '12px', color: '#666' }
            },
            xaxis: {
                type: 'datetime',
                title: {
                    text: 'Date',
                    style: { fontSize: '14px' }
                },
                labels: {
                    style: { fontSize: '10px' },
                    format: 'MMM dd'
                },
                axisBorder: {
                    show: true
                },
                axisTicks: {
                    show: true
                }
            },
            yaxis: {
                title: {
                    text: 'Hour (JST)',
                    style: { fontSize: '14px' }
                },
                labels: {
                    style: { fontSize: '10px' }
                },
                reversed: true // 00:00 at top, 23:00 at bottom
            },
            plotOptions: {
                heatmap: {
                    shadeIntensity: 0.5,
                    radius: 3,
                    useFillColorAsStroke: false,
                    colorScale: {
                        inverse: false,
                        ranges: [
                            // Default range - we'll override colors via events
                            {
                                from: 0,
                                to: 0,
                                name: 'No Activity',
                                color: '#F3F4F6'
                            }
                        ]
                    }
                }
            },
            // Custom color function to handle school vs non-school time
            colors: ['#239A3B'], // Default school time color
            fill: {
                type: 'gradient',
                gradient: {
                    colorStops: []
                }
            },
            tooltip: {
                custom: function({series, seriesIndex, dataPointIndex, w}) {
                    const hourName = w.globals.seriesNames[seriesIndex];
                    const dateValue = w.globals.categoryLabels[dataPointIndex];
                    const value = series[seriesIndex][dataPointIndex];

                    // Get the original data point to check if it's school time
                    const dataPoint = hourlyHeatmapData.combined_series[seriesIndex].data[dataPointIndex];
                    const isSchoolTime = dataPoint.school_time;

                    // Format date nicely - handle different date formats
                    let dateStr = 'Invalid Date';
                    let dateIsoString = null;
                    try {
                        // Try to get the original date from the data point first
                        const originalDateStr = dataPoint.x;
                        let date;

                        if (originalDateStr) {
                            // Parse the ISO date string from our data
                            date = new Date(originalDateStr);
                            dateIsoString = originalDateStr; // Store for holiday lookup
                        } else {
                            // Fallback to the category label
                            date = new Date(dateValue);
                            if (!isNaN(date.getTime())) {
                                dateIsoString = date.toISOString().split('T')[0]; // Convert to YYYY-MM-DD format
                            }
                        }

                        // Check if date is valid
                        if (!isNaN(date.getTime())) {
                            dateStr = date.toLocaleDateString('en-US', {
                                weekday: 'short',
                                month: 'short',
                                day: 'numeric'
                            });
                        } else {
                            // Last resort: try to parse as timestamp
                            const timestamp = parseInt(dateValue);
                            if (!isNaN(timestamp)) {
                                date = new Date(timestamp);
                                dateStr = date.toLocaleDateString('en-US', {
                                    weekday: 'short',
                                    month: 'short',
                                    day: 'numeric'
                                });
                                dateIsoString = date.toISOString().split('T')[0];
                            }
                        }
                    } catch (e) {
                        console.warn('Error parsing date in tooltip:', e, 'dateValue:', dateValue);
                        dateStr = 'Date unavailable';
                    }

                    // Check if this date is a holiday
                    const holidayInfo = hourlyHeatmapData.holiday_info || {};
                    const holidayName = dateIsoString ? holidayInfo[dateIsoString] : null;

                    const timeType = isSchoolTime ? 'School Time' : 'Non-School Time';
                    const colorClass = isSchoolTime ? 'text-green-600 dark:text-green-400' : 'text-orange-600 dark:text-orange-400';

                    // Build tooltip HTML
                    let tooltipHTML = `
                        <div class="p-3 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-600 rounded shadow-lg">
                            <div class="font-semibold text-gray-900 dark:text-white">${dateStr}</div>
                    `;

                    // Add holiday information if it exists
                    if (holidayName) {
                        tooltipHTML += `
                            <div style="background-color: #fed7aa; color: #9a3412; font-weight: 600; padding: 4px 8px; border-radius: 4px; margin-top: 4px; border-left: 3px solid #ea580c;">
                                <i class="ri-calendar-event-line" style="margin-right: 4px;"></i>${holidayName}
                            </div>
                        `;
                    }

                    tooltipHTML += `
                            <div class="text-sm text-gray-600 dark:text-gray-300 mt-1">Time: ${hourName}</div>
                            <div class="text-sm ${colorClass}">${timeType}</div>
                            <div class="text-sm font-medium ${colorClass}">Activities: ${value}</div>
                    `;

                    // Add student count and per-student metrics if available
                    if (dataPoint.student_count !== undefined) {
                        const studentCount = dataPoint.student_count;
                        const activitiesPerStudent = studentCount > 0 ? (value / studentCount).toFixed(1) : '0';

                        tooltipHTML += `
                            <div class="text-sm font-medium text-blue-600 dark:text-blue-400">Students: ${studentCount}</div>
                            <div class="text-sm text-gray-600 dark:text-gray-300">Avg per student: ${activitiesPerStudent}</div>
                        `;

                        // Add engagement quality indicator
                        let engagementLabel = '';
                        let engagementColor = '';

                        if (studentCount === 0) {
                            engagementLabel = 'No activity';
                            engagementColor = 'text-gray-500';
                        } else {
                            const activitiesPerStudentFloat = parseFloat(activitiesPerStudent);
                            const thresholds = engagementThresholds || {high: 10, moderate: 5, light: 1, brief: 0.1};

                            if (activitiesPerStudentFloat >= thresholds.high) {
                                engagementLabel = `High engagement (top 25%)`;
                                engagementColor = 'text-green-600 dark:text-green-400';
                            } else if (activitiesPerStudentFloat >= thresholds.moderate) {
                                engagementLabel = `Moderate engagement (above median)`;
                                engagementColor = 'text-yellow-600 dark:text-yellow-400';
                            } else if (activitiesPerStudentFloat >= thresholds.light) {
                                engagementLabel = `Light engagement (above 25th percentile)`;
                                engagementColor = 'text-orange-600 dark:text-orange-400';
                            } else if (activitiesPerStudentFloat >= thresholds.brief) {
                                engagementLabel = studentCount > 5 ? 'Brief interactions' : 'Minimal engagement';
                                engagementColor = 'text-red-600 dark:text-red-400';
                            } else {
                                engagementLabel = 'Very low participation';
                                engagementColor = 'text-gray-500';
                            }
                        }

                        tooltipHTML += `
                            <div class="text-xs ${engagementColor} font-medium mt-1 italic">${engagementLabel}</div>
                        `;
                    }

                    tooltipHTML += `
                        </div>
                    `;

                    return tooltipHTML;
                }
            },
            grid: {
                borderColor: '#e7e7e7',
                strokeDashArray: 0,
                padding: {
                    right: 20
                }
            },
            // Add week separations if available
            annotations: hourlyHeatmapData.week_boundaries ? {
                xaxis: hourlyHeatmapData.week_boundaries.map(date => ({
                    x: new Date(date).getTime(),
                    strokeDashArray: 2,
                    borderColor: '#999',
                    opacity: 0.3
                }))
            } : {}
        };

        // Apply custom coloring based on school vs non-school time
        if (hourlyHeatmapData.combined_series) {
            // Create chart events to apply colors
            combinedHeatmapOptions.chart.events = {
                dataPointSelection: function(event, chartContext, config) {
                    // Optional: handle data point selection
                },
                mounted: function(chartContext, config) {
                    console.log('Heatmap mounted, applying custom colors...');
                    window.applyHeatmapColors();
                },
                updated: function(chartContext, config) {
                    console.log('Heatmap updated, reapplying custom colors...');
                    window.applyHeatmapColors();
                }
            };
        }

        // Custom heatmap rendering to store chart reference
        try {
            const heatmapElement = document.querySelector("#combined-activity-heatmap");
            if (!heatmapElement) {
                console.error('Heatmap element not found');
                return;
            }

            heatmapElement.innerHTML = '';
            currentHeatmapChart = new ApexCharts(heatmapElement, combinedHeatmapOptions);
            currentHeatmapChart.render().then(() => {
                console.log('Combined activity heatmap rendered successfully');

                // Add event listener for toggle button
                const toggleBtn = document.getElementById('heatmap-toggle-btn');
                if (toggleBtn) {
                    toggleBtn.addEventListener('click', toggleDetailedHeatmapView);
                }
            }).catch((error) => {
                console.error('Error rendering combined activity heatmap:', error);
                heatmapElement.innerHTML = '<div class="flex items-center justify-center h-96 text-red-500">Error loading combined activity heatmap</div>';
            });
        } catch (error) {
            console.error('Error initializing combined activity heatmap:', error);
            const heatmapElement = document.querySelector("#combined-activity-heatmap");
            if (heatmapElement) {
                heatmapElement.innerHTML = '<div class="flex items-center justify-center h-96 text-red-500">Error loading combined activity heatmap</div>';
            }
        }

    } else {
        document.querySelector("#combined-activity-heatmap").innerHTML =
            '<div class="flex items-center justify-center h-96 text-gray-500"><div class="text-center"><i class="ri-calendar-2-line text-4xl mb-2"></i><p>No hourly activity data available</p></div></div>';
    }

    // Daily Trends Chart
    if (analyticsData.daily_trends && analyticsData.daily_trends.length > 0) {
        const dailyTrendsOptions = {
            series: [{
                name: 'Active Students',
                type: 'column',
                data: analyticsData.daily_trends.map(item => ({
                    x: item.date || 'Unknown',
                    y: item.active_students || 0
                }))
            }, {
                name: 'Total Activities',
                type: 'line',
                data: analyticsData.daily_trends.map(item => ({
                    x: item.date || 'Unknown',
                    y: item.total_activities || 0
                }))
            }],
            chart: {
                height: 384,
                type: 'line',
                toolbar: { show: false },
                background: 'transparent'
            },
            stroke: {
                width: [0, 4],
                curve: 'smooth'
            },
            dataLabels: { enabled: false },
            xaxis: {
                type: 'datetime',
                title: {
                    text: 'Date',
                    style: { fontSize: '14px' }
                },
                labels: {
                    style: { fontSize: '12px' }
                }
            },
            yaxis: [{
                title: {
                    text: 'Active Students',
                    style: { fontSize: '14px' }
                },
                labels: {
                    style: { fontSize: '12px' }
                }
            }, {
                opposite: true,
                title: {
                    text: 'Total Activities',
                    style: { fontSize: '14px' }
                },
                labels: {
                    style: { fontSize: '12px' }
                }
            }],
            colors: ['#3B82F6', '#10B981'],
            title: {
                text: 'Daily Learning Activity Trends',
                align: 'left',
                style: { fontSize: '16px', fontWeight: 'bold' }
            },
            grid: {
                borderColor: '#e7e7e7',
                strokeDashArray: 3
            },
            legend: {
                position: 'top',
                fontSize: '12px'
            }
        };
        renderChart("#daily-trends-chart", dailyTrendsOptions, "Error loading daily trends chart");
    } else {
        document.querySelector("#daily-trends-chart").innerHTML =
            '<div class="flex items-center justify-center h-96 text-gray-500"><div class="text-center"><i class="ri-line-chart-line text-4xl mb-2"></i><p>No daily trends data available</p></div></div>';
    }

    // Initialize counter animations with proper error handling
    document.querySelectorAll('.counter-value').forEach(counter => {
        const dataValue = counter.getAttribute('data-value');
        let target = 0;

        // Handle various data types and edge cases
        if (dataValue && dataValue !== 'None' && dataValue !== 'null' && dataValue !== 'undefined') {
            const parsed = parseFloat(dataValue);
            if (!isNaN(parsed)) {
                target = parsed;
            }
        }

        // Set initial value
        counter.textContent = target;

        // Animate if target is reasonable
        if (target > 0 && target < 10000) {
            let current = 0;
            const increment = target / 50;
            const timer = setInterval(() => {
                current += increment;
                if (current >= target) {
                    counter.textContent = target % 1 === 0 ? target : target.toFixed(2);
                    clearInterval(timer);
                } else {
                    counter.textContent = Math.floor(current);
                }
            }, 20);
        }
    });

    // Set progress bar widths
    document.querySelectorAll('.progress-bar').forEach(progressBar => {
        const width = progressBar.getAttribute('data-width');
        if (width !== null && width !== undefined) {
            progressBar.style.width = width + '%';
        }
    });

    console.log('Student Analytics Dashboard initialization complete');
});

// Global variable to store current heatmap state
let isDetailedHeatmapView = false;
let currentHeatmapChart = null;
let combinedHeatmapOptions = null;
let engagementThresholds = null; // Store calculated engagement thresholds

// Toggle function for detailed heatmap view
function toggleDetailedHeatmapView() {
    const toggleBtn = document.getElementById('heatmap-toggle-btn');
    const heatmapContainer = document.querySelector('#combined-activity-heatmap');

    if (!toggleBtn || !heatmapContainer) return;

    isDetailedHeatmapView = !isDetailedHeatmapView;

    // Update button text and icon
    if (isDetailedHeatmapView) {
        toggleBtn.innerHTML = '<i class="ri-eye-line mr-1"></i>Simple View';
        toggleBtn.title = 'Switch to simple color view';
    } else {
        toggleBtn.innerHTML = '<i class="ri-list-check-line mr-1"></i>Show Details';
        toggleBtn.title = 'Toggle detailed view with numbers in cells';
    }

    // Recreate the heatmap with updated configuration
    if (currentHeatmapChart) {
        currentHeatmapChart.destroy();
    }

    // Update the heatmap options for detailed view
    const updatedOptions = { ...combinedHeatmapOptions };

    if (isDetailedHeatmapView) {
        updatedOptions.dataLabels = {
            enabled: true,
            style: {
                fontSize: '9px',
                colors: ['#000']
            },
            formatter: function(value, { seriesIndex, dataPointIndex, w }) {
                if (!hourlyHeatmapData.combined_series || !hourlyHeatmapData.combined_series[seriesIndex]) {
                    return value || '';
                }

                const dataPoint = hourlyHeatmapData.combined_series[seriesIndex].data[dataPointIndex];
                if (!dataPoint || value === 0) return '';

                const studentCount = dataPoint.student_count || 0;
                if (studentCount === 0) return value;

                // Show format: "activities/students"
                return `${value}/${studentCount}`;
            }
        };

        // Adjust cell size for better text readability
        updatedOptions.plotOptions.heatmap.radius = 2;
        updatedOptions.chart.height = 700; // Make it taller for better readability
    } else {
        updatedOptions.dataLabels = { enabled: false };
        updatedOptions.plotOptions.heatmap.radius = 3;
        updatedOptions.chart.height = 600;
    }

    // Re-render the chart
    try {
        currentHeatmapChart = new ApexCharts(heatmapContainer, updatedOptions);
        currentHeatmapChart.render().then(() => {
            console.log('Heatmap toggled to', isDetailedHeatmapView ? 'detailed' : 'simple', 'view');

            // Ensure engagement thresholds are available for tooltips
            if (!engagementThresholds && hourlyHeatmapData.combined_series) {
                // Recalculate thresholds if they weren't set initially
                const activitiesPerStudentValues = [];
                hourlyHeatmapData.combined_series.forEach(hourSeries => {
                    hourSeries.data.forEach(dataPoint => {
                        if (dataPoint.student_count > 0 && dataPoint.y > 0) {
                            const activitiesPerStudent = dataPoint.y / dataPoint.student_count;
                            activitiesPerStudentValues.push(activitiesPerStudent);
                        }
                    });
                });

                if (activitiesPerStudentValues.length > 0) {
                    const sortedValues = activitiesPerStudentValues.sort((a, b) => a - b);
                    const q1 = sortedValues[Math.floor(sortedValues.length * 0.25)];
                    const q2 = sortedValues[Math.floor(sortedValues.length * 0.50)];
                    const q3 = sortedValues[Math.floor(sortedValues.length * 0.75)];

                    engagementThresholds = {
                        high: q3,
                        moderate: q2,
                        light: q1,
                        brief: Math.min(q1 / 2, sortedValues[0] > 0 ? sortedValues[0] : 0.1)
                    };
                    console.log('Recalculated engagement thresholds in toggle:', engagementThresholds);
                }
            }

            // Reapply custom colors after render
            if (hourlyHeatmapData.combined_series && window.applyHeatmapColors) {
                console.log('Toggle function: reapplying custom colors...');
                window.applyHeatmapColors();
            }
        });
    } catch (error) {
        console.error('Error toggling heatmap view:', error);
    }
}

// Toggle function for legend show/hide
function toggleLegend() {
    const content = document.getElementById('legend-content');
    const arrow = document.getElementById('legend-arrow');

    if (content.classList.contains('hidden')) {
        // Show content
        content.classList.remove('hidden');
        arrow.classList.add('rotate-180');
    } else {
        // Hide content
        content.classList.add('hidden');
        arrow.classList.remove('rotate-180');
    }
}

// Toggle function for interpretation guide show/hide
function toggleInterpretationGuide() {
    const content = document.getElementById('interpretation-content');
    const arrow = document.getElementById('interpretation-arrow');

    if (content.classList.contains('hidden')) {
        // Show content
        content.classList.remove('hidden');
        arrow.classList.add('rotate-180');
    } else {
        // Hide content
        content.classList.add('hidden');
        arrow.classList.remove('rotate-180');
    }
}