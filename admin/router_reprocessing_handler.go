package main

import (
	"net/http"
	"sort"
	"time"

	"github.com/gin-gonic/gin"
)

// ReprocessingStartRequest represents a request to start a reprocessing job
type ReprocessingStartRequest struct {
	S3Path         string    `json:"s3_path,omitempty"`
	LocalPath      string    `json:"local_path,omitempty"`
	StreamIds      []string  `json:"stream_ids,omitempty"`
	ConnectionIds  []string  `json:"connection_ids,omitempty"`
	DryRun         bool      `json:"dry_run"`
	StartFile      string    `json:"start_file,omitempty"`
	StartLine      int64     `json:"start_line,omitempty"`
	MaxConcurrency int       `json:"max_concurrency,omitempty"`
	BatchSize      int       `json:"batch_size,omitempty"`
	DateFrom       time.Time `json:"date_from,omitempty"`
	DateTo         time.Time `json:"date_to,omitempty"`
	Limit          int64     `json:"limit,omitempty"`
}

// ReprocessingJobResponse represents a job in API responses
type ReprocessingJobResponse struct {
	ID               string                `json:"id"`
	Status           ReprocessingJobStatus `json:"status"`
	Config           ReprocessingJobConfig `json:"config"`
	CreatedAt        string                `json:"created_at"`
	StartedAt        string                `json:"started_at,omitempty"`
	CompletedAt      string                `json:"completed_at,omitempty"`
	CurrentFile      string                `json:"current_file,omitempty"`
	CurrentLine      int64                 `json:"current_line"`
	CurrentTimestamp string                `json:"current_timestamp,omitempty"`
	TotalFiles       int                   `json:"total_files"`
	ProcessedFiles   int                   `json:"processed_files"`
	TotalLines       int64                 `json:"total_lines"`
	ProcessedLines   int64                 `json:"processed_lines"`
	SuccessCount     int64                 `json:"success_count"`
	ErrorCount       int64                 `json:"error_count"`
	SkippedCount     int64                 `json:"skipped_count"`
	LastError        string                `json:"last_error,omitempty"`
	Progress         float64               `json:"progress"`
}

// jobToResponse converts a ReprocessingJob to a response format
func jobToResponse(job *ReprocessingJob) ReprocessingJobResponse {
	resp := ReprocessingJobResponse{
		ID:               job.ID,
		Status:           job.Status,
		Config:           job.Config,
		CreatedAt:        job.CreatedAt.Format("2006-01-02T15:04:05Z"),
		CurrentFile:      job.CurrentFile,
		CurrentLine:      job.CurrentLine,
		CurrentTimestamp: job.CurrentTimestamp,
		TotalFiles:       job.TotalFiles,
		ProcessedFiles:   job.ProcessedFiles,
		TotalLines:       job.TotalLines,
		ProcessedLines:   job.ProcessedLines,
		SuccessCount:     job.SuccessCount,
		ErrorCount:       job.ErrorCount,
		SkippedCount:     job.SkippedCount,
		LastError:        job.LastError,
	}

	if job.StartedAt != nil {
		resp.StartedAt = job.StartedAt.Format("2006-01-02T15:04:05Z")
	}
	if job.CompletedAt != nil {
		resp.CompletedAt = job.CompletedAt.Format("2006-01-02T15:04:05Z")
	}

	// Calculate progress
	if job.TotalFiles > 0 {
		resp.Progress = float64(job.ProcessedFiles) / float64(job.TotalFiles)
	}

	return resp
}

// startReprocessingJob starts a new reprocessing job
func (r *Router) startReprocessingJob(c *gin.Context) {
	var req ReprocessingStartRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}


	// Get or create reprocessing manager
	manager := r.getReprocessingManager()
	if manager == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "reprocessing manager not initialized"})
		return
	}

	// Create job config
	config := ReprocessingJobConfig{
		S3Path:         req.S3Path,
		LocalPath:      req.LocalPath,
		StreamIds:      req.StreamIds,
		ConnectionIds:  req.ConnectionIds,
		DryRun:         req.DryRun,
		StartFile:      req.StartFile,
		StartLine:      req.StartLine,
		MaxConcurrency: req.MaxConcurrency,
		BatchSize:      req.BatchSize,
		DateFrom:       req.DateFrom,
		DateTo:         req.DateTo,
		Limit:          req.Limit,
	}

	// Start job
	job, err := manager.StartJob(config)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, jobToResponse(job))
}

// listReprocessingJobs lists all reprocessing jobs
func (r *Router) listReprocessingJobs(c *gin.Context) {

	manager := r.getReprocessingManager()
	if manager == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "reprocessing manager not initialized"})
		return
	}

	jobs := manager.ListJobs()
	
	// Sort jobs by created date (newest first)
	sort.Slice(jobs, func(i, j int) bool {
		return jobs[i].CreatedAt.After(jobs[j].CreatedAt)
	})
	
	responses := make([]ReprocessingJobResponse, len(jobs))
	for i, job := range jobs {
		responses[i] = jobToResponse(job)
	}

	c.JSON(http.StatusOK, gin.H{"jobs": responses})
}

// getReprocessingJob gets a specific reprocessing job
func (r *Router) getReprocessingJob(c *gin.Context) {
	jobId := c.Param("id")
	if jobId == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "job id required"})
		return
	}


	manager := r.getReprocessingManager()
	if manager == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "reprocessing manager not initialized"})
		return
	}

	job, err := manager.GetJob(jobId)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, jobToResponse(job))
}

// pauseReprocessingJob pauses a running job
func (r *Router) pauseReprocessingJob(c *gin.Context) {
	jobId := c.Param("id")
	if jobId == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "job id required"})
		return
	}


	manager := r.getReprocessingManager()
	if manager == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "reprocessing manager not initialized"})
		return
	}

	if err := manager.PauseJob(jobId); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	job, _ := manager.GetJob(jobId)
	c.JSON(http.StatusOK, jobToResponse(job))
}

// resumeReprocessingJob resumes a paused job
func (r *Router) resumeReprocessingJob(c *gin.Context) {
	jobId := c.Param("id")
	if jobId == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "job id required"})
		return
	}


	manager := r.getReprocessingManager()
	if manager == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "reprocessing manager not initialized"})
		return
	}

	if err := manager.ResumeJob(jobId); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	job, _ := manager.GetJob(jobId)
	c.JSON(http.StatusOK, jobToResponse(job))
}

// cancelReprocessingJob cancels a job
func (r *Router) cancelReprocessingJob(c *gin.Context) {
	jobId := c.Param("id")
	if jobId == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "job id required"})
		return
	}


	manager := r.getReprocessingManager()
	if manager == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "reprocessing manager not initialized"})
		return
	}

	if err := manager.CancelJob(jobId); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	job, _ := manager.GetJob(jobId)
	c.JSON(http.StatusOK, jobToResponse(job))
}

// getReprocessingManager gets or creates the reprocessing manager
func (r *Router) getReprocessingManager() *ReprocessingJobManager {
	return r.reprocessingManager
}

// serveAdminHTML serves the HTML interface for admin
func (r *Router) serveAdminHTML(c *gin.Context) {
	htmlContent := `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Bulker Admin</title>
    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
            background-color: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        h1 {
            margin-top: 0;
            color: #333;
        }
        .auth-section {
            background-color: #f9f9f9;
            padding: 15px;
            border-radius: 4px;
            margin-bottom: 20px;
        }
        .auth-section input[type="password"] {
            width: 300px;
            padding: 8px;
            margin-left: 10px;
            border: 1px solid #ddd;
            border-radius: 4px;
        }
        .section {
            margin-bottom: 30px;
        }
        .job-form {
            background-color: #f9f9f9;
            padding: 15px;
            border-radius: 4px;
            margin-bottom: 20px;
        }
        .form-group {
            margin-bottom: 15px;
        }
        .form-group label {
            display: inline-block;
            width: 150px;
            font-weight: bold;
        }
        .form-group input, .form-group textarea {
            width: 300px;
            padding: 5px;
            border: 1px solid #ddd;
            border-radius: 4px;
        }
        .form-group textarea {
            vertical-align: top;
            height: 60px;
        }
        .form-group input[type="checkbox"] {
            width: auto;
        }
        .date-help {
            font-size: 12px;
            color: #666;
            margin-top: 5px;
        }
        table {
            width: 100%;
            border-collapse: collapse;
        }
        th, td {
            text-align: left;
            padding: 10px;
            border-bottom: 1px solid #ddd;
        }
        th {
            background-color: #f0f0f0;
            font-weight: bold;
        }
        .status {
            padding: 3px 8px;
            border-radius: 3px;
            font-size: 12px;
            font-weight: bold;
        }
        .status-pending { background-color: #ffc107; color: white; }
        .status-running { background-color: #28a745; color: white; }
        .status-paused { background-color: #17a2b8; color: white; }
        .status-completed { background-color: #6c757d; color: white; }
        .status-failed { background-color: #dc3545; color: white; }
        .status-cancelled { background-color: #6c757d; color: white; }
        .actions button {
            margin-right: 5px;
            padding: 5px 10px;
            font-size: 12px;
            border: none;
            border-radius: 3px;
            cursor: pointer;
        }
        .btn-pause { background-color: #17a2b8; color: white; }
        .btn-resume { background-color: #28a745; color: white; }
        .btn-cancel { background-color: #dc3545; color: white; }
        .btn-refresh { background-color: #007bff; color: white; }
        .progress-bar {
            width: 100px;
            height: 10px;
            background-color: #e0e0e0;
            border-radius: 5px;
            overflow: hidden;
        }
        .progress-fill {
            height: 100%;
            background-color: #28a745;
            transition: width 0.3s ease;
        }
        .error-message {
            color: #dc3545;
            margin-top: 10px;
            padding: 10px;
            background-color: #f8d7da;
            border-radius: 4px;
            display: none;
        }
        .success-message {
            color: #155724;
            margin-top: 10px;
            padding: 10px;
            background-color: #d4edda;
            border-radius: 4px;
            display: none;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>Bulker Admin - Reprocessing Jobs</h1>
        
        <div class="auth-section">
            <label for="authToken">Auth Token:</label>
            <input type="password" id="authToken" placeholder="Enter your auth token">
        </div>

        <div class="section">
            <h2>Start New Reprocessing Job</h2>
            <div class="job-form">
                <div class="form-group">
                    <label for="s3Path">S3 Path:</label>
                    <input type="text" id="s3Path" placeholder="s3://bucket/path">
                </div>
                <div class="form-group">
                    <label for="localPath">Local Path:</label>
                    <input type="text" id="localPath" placeholder="/path/to/files">
                </div>
                <div class="form-group">
                    <label for="streamIds">Stream IDs:</label>
                    <textarea id="streamIds" placeholder="One per line"></textarea>
                </div>
                <div class="form-group">
                    <label for="connectionIds">Connection IDs:</label>
                    <textarea id="connectionIds" placeholder="One per line"></textarea>
                </div>
                <div class="form-group">
                    <label for="startFile">Start File:</label>
                    <input type="text" id="startFile" placeholder="Optional">
                </div>
                <div class="form-group">
                    <label for="startLine">Start Line:</label>
                    <input type="number" id="startLine" placeholder="0">
                </div>
                <div class="form-group">
                    <label for="maxConcurrency">Max Concurrency:</label>
                    <input type="number" id="maxConcurrency" placeholder="10">
                </div>
                <div class="form-group">
                    <label for="batchSize">Batch Size:</label>
                    <input type="number" id="batchSize" placeholder="1000">
                </div>
                <div class="form-group">
                    <label for="dateFrom">Date From (UTC):</label>
                    <input type="text" id="dateFrom" placeholder="YYYY-MM-DDTHH:MM:SSZ">
                    <div class="date-help">Example: 2024-01-15T10:30:00Z</div>
                </div>
                <div class="form-group">
                    <label for="dateTo">Date To (UTC):</label>
                    <input type="text" id="dateTo" placeholder="YYYY-MM-DDTHH:MM:SSZ">
                    <div class="date-help">Example: 2024-01-16T18:45:00Z</div>
                </div>
                <div class="form-group">
                    <label for="limit">Event Limit:</label>
                    <input type="number" id="limit" placeholder="Optional (e.g. 10000)">
                </div>
                <div class="form-group">
                    <label for="dryRun">Dry Run:</label>
                    <input type="checkbox" id="dryRun">
                </div>
                <button onclick="startJob()" style="background-color: #28a745; color: white; padding: 10px 20px; border: none; border-radius: 4px; cursor: pointer;">Start Job</button>
            </div>
        </div>

        <div class="section">
            <h2>Running Jobs</h2>
            <button onclick="refreshJobs()" class="btn-refresh">Refresh</button>
            <div id="jobsTable" style="margin-top: 15px;">
                <table>
                    <thead>
                        <tr>
                            <th>ID</th>
                            <th>Status</th>
                            <th>Progress</th>
                            <th>Files</th>
                            <th>Lines</th>
                            <th>Success/Error/Skipped</th>
                            <th>Current File</th>
                            <th>Actions</th>
                        </tr>
                    </thead>
                    <tbody id="jobsTableBody">
                    </tbody>
                </table>
            </div>
        </div>

        <div class="error-message" id="errorMessage"></div>
        <div class="success-message" id="successMessage"></div>
    </div>

    <script>
        function getAuthToken() {
            return document.getElementById('authToken').value;
        }
        
        function validateISODate(dateStr) {
            if (!dateStr) return true; // Empty is valid (optional field)
            const regex = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(Z|[+-]\d{2}:\d{2})$/;
            return regex.test(dateStr);
        }

        function showError(message) {
            const errorDiv = document.getElementById('errorMessage');
            errorDiv.textContent = message;
            errorDiv.style.display = 'block';
            setTimeout(() => {
                errorDiv.style.display = 'none';
            }, 5000);
        }

        function showSuccess(message) {
            const successDiv = document.getElementById('successMessage');
            successDiv.textContent = message;
            successDiv.style.display = 'block';
            setTimeout(() => {
                successDiv.style.display = 'none';
            }, 3000);
        }

        async function apiCall(method, endpoint, body = null) {
            const token = getAuthToken();
            if (!token) {
                showError('Please enter auth token');
                return null;
            }

            try {
                const options = {
                    method: method,
                    headers: {
                        'Authorization': 'Bearer ' + token,
                        'Content-Type': 'application/json'
                    }
                };
                
                if (body) {
                    options.body = JSON.stringify(body);
                }

                const response = await fetch('/api/admin/reprocessing' + endpoint, options);
                const data = await response.json();
                
                if (!response.ok) {
                    throw new Error(data.error || 'API request failed');
                }
                
                return data;
            } catch (error) {
                showError('API Error: ' + error.message);
                return null;
            }
        }

        async function startJob() {
            const streamIds = document.getElementById('streamIds').value.split('\n').filter(id => id.trim());
            const connectionIds = document.getElementById('connectionIds').value.split('\n').filter(id => id.trim());
            
            // Validate date formats
            const dateFrom = document.getElementById('dateFrom').value;
            const dateTo = document.getElementById('dateTo').value;
            
            if (dateFrom && !validateISODate(dateFrom)) {
                showError('Date From must be in ISO format: YYYY-MM-DDTHH:MM:SSZ');
                return;
            }
            
            if (dateTo && !validateISODate(dateTo)) {
                showError('Date To must be in ISO format: YYYY-MM-DDTHH:MM:SSZ');
                return;
            }
            
            const jobData = {
                s3_path: document.getElementById('s3Path').value || undefined,
                local_path: document.getElementById('localPath').value || undefined,
                stream_ids: streamIds.length > 0 ? streamIds : undefined,
                connection_ids: connectionIds.length > 0 ? connectionIds : undefined,
                start_file: document.getElementById('startFile').value || undefined,
                start_line: parseInt(document.getElementById('startLine').value) || 0,
                max_concurrency: parseInt(document.getElementById('maxConcurrency').value) || undefined,
                batch_size: parseInt(document.getElementById('batchSize').value) || undefined,
                date_from: document.getElementById('dateFrom').value || undefined,
                date_to: document.getElementById('dateTo').value || undefined,
                limit: parseInt(document.getElementById('limit').value) || undefined,
                dry_run: document.getElementById('dryRun').checked
            };

            // Remove undefined values
            Object.keys(jobData).forEach(key => jobData[key] === undefined && delete jobData[key]);

            const result = await apiCall('POST', '/jobs', jobData);
            if (result) {
                showSuccess('Job started: ' + result.id);
                refreshJobs();
                // Clear form
                document.querySelectorAll('.job-form input[type="text"], .job-form textarea').forEach(el => el.value = '');
                document.querySelectorAll('.job-form input[type="number"]').forEach(el => el.value = '');
                document.getElementById('dryRun').checked = false;
            }
        }

        async function refreshJobs() {
            const result = await apiCall('GET', '/jobs');
            if (result && result.jobs) {
                displayJobs(result.jobs);
            }
        }

        function displayJobs(jobs) {
            const tbody = document.getElementById('jobsTableBody');
            tbody.innerHTML = '';
            
            jobs.forEach(job => {
                const row = document.createElement('tr');
                const progress = job.progress * 100;
                
                row.innerHTML = ` + "`" + `
                    <td>${job.id.substring(0, 8)}...</td>
                    <td><span class="status status-${job.status}">${job.status}</span></td>
                    <td>
                        <div class="progress-bar">
                            <div class="progress-fill" style="width: ${progress}%"></div>
                        </div>
                        ${progress.toFixed(1)}%
                    </td>
                    <td>${job.processed_files}/${job.total_files}</td>
                    <td>${job.processed_lines}/${job.total_lines}</td>
                    <td>${job.success_count}/${job.error_count}/${job.skipped_count}</td>
                    <td>${job.current_file || '-'}</td>
                    <td class="actions">
                        ${job.status === 'running' ? '<button class="btn-pause" onclick="pauseJob(\'' + job.id + '\')">Pause</button>' : ''}
                        ${job.status === 'paused' ? '<button class="btn-resume" onclick="resumeJob(\'' + job.id + '\')">Resume</button>' : ''}
                        ${job.status !== 'completed' && job.status !== 'cancelled' && job.status !== 'failed' ? '<button class="btn-cancel" onclick="cancelJob(\'' + job.id + '\')">Cancel</button>' : ''}
                    </td>
                ` + "`" + `;
                tbody.appendChild(row);
            });
        }

        async function pauseJob(jobId) {
            const result = await apiCall('POST', '/jobs/' + jobId + '/pause');
            if (result) {
                showSuccess('Job paused');
                refreshJobs();
            }
        }

        async function resumeJob(jobId) {
            const result = await apiCall('POST', '/jobs/' + jobId + '/resume');
            if (result) {
                showSuccess('Job resumed');
                refreshJobs();
            }
        }

        async function cancelJob(jobId) {
            if (confirm('Are you sure you want to cancel this job?')) {
                const result = await apiCall('POST', '/jobs/' + jobId + '/cancel');
                if (result) {
                    showSuccess('Job cancelled');
                    refreshJobs();
                }
            }
        }

        // Auto-refresh jobs every 5 seconds
        setInterval(refreshJobs, 5000);
        
        // Initial load
        refreshJobs();
    </script>
</body>
</html>`
	
	c.Header("Content-Type", "text/html")
	c.String(http.StatusOK, htmlContent)
}