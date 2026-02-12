// CarrotQuant Dashboard JS (Enhanced)
let registry = {};
let metadata = {};

async function fetchApi(path, options = {}) {
    const response = await fetch('/api/v1' + path, options);
    if (!response.ok) throw new Error(await response.text());
    return response.json();
}

async function initDashboard() {
    try {
        // 1. Fetch data in parallel
        [registry, metadata] = await Promise.all([
            fetchApi('/market/registry'),
            fetchApi('/market/tables')
        ]);
        renderAuditTable();
        refreshTasks(); // Initial task fetch
    } catch (e) {
        console.error('Initialization failed', e);
    }
}

function renderAuditTable() {
    const tbody = document.getElementById('audit-body');
    if (!tbody) return;
    tbody.innerHTML = '';

    for (const [t_name, config] of Object.entries(registry)) {
        const info = metadata[t_name];
        const hasData = !!info;

        const statusClass = hasData ? 'status-ok' : 'status-missing';
        const statusText = hasData ? '已就绪' : '缺失';

        const row = `<tr>
            <td><code class="text-primary">${t_name}</code></td>
            <td>${config.storage_type}</td>
            <td>${hasData ? info.row_count : '0'}</td>
            <td>${hasData ? (info.start_date + ' ~ ' + info.end_date) : '-'}</td>
            <td class="${statusClass}">${statusText}</td>
            <td>
                <button class="btn btn-sm btn-outline-primary" onclick="openQuickDownload('${t_name}')">
                    ${hasData ? '补充下载' : '立即下载'}
                </button>
            </td>
        </tr>`;
        tbody.insertAdjacentHTML('beforeend', row);
    }
}

async function refreshTasks() {
    try {
        const tasks = await fetchApi('/market/tasks');
        renderTaskList(tasks);
    } catch (e) {
        console.error('Refresh tasks failed', e);
    }
}

function renderTaskList(tasks) {
    const container = document.getElementById('task-list-container');
    if (!container) return;

    if (tasks.length === 0) {
        container.innerHTML = '<p class="text-center text-muted">目前没有下载任务</p>';
        return;
    }

    // Sort tasks by updated_at descending
    tasks.sort((a, b) => new Date(b.updated_at) - new Date(a.updated_at));

    let html = '';
    tasks.forEach(task => {
        const statusBadge = getStatusBadge(task.status);
        html += `
        <div class="mb-4 border-bottom pb-3">
            <div class="d-flex justify-content-between align-items-center mb-2">
                <h6 class="mb-0 text-monospace">${task.task_id.substring(0, 8)}...</h6>
                ${statusBadge}
            </div>
            <p class="small text-muted mb-2">${task.message}</p>
            <div class="progress" style="height: 10px;">
                <div class="progress-bar progress-bar-striped progress-bar-animated" 
                     role="progressbar" 
                     style="width: ${task.progress}%" 
                     aria-valuenow="${task.progress}" 
                     aria-valuemin="0" 
                     aria-valuemax="100"></div>
            </div>
            <div class="d-flex justify-content-between mt-1">
                <span class="small text-muted">${task.progress}%</span>
                <span class="small text-muted">${new Date(task.updated_at).toLocaleTimeString()}</span>
            </div>
        </div>`;
    });
    container.innerHTML = html;
}

function getStatusBadge(status) {
    switch (status) {
        case 'pending': return '<span class="badge bg-secondary">等待中</span>';
        case 'running': return '<span class="badge bg-primary">进行中</span>';
        case 'completed': return '<span class="badge bg-success">已完成</span>';
        case 'failed': return '<span class="badge bg-danger">失败</span>';
        case 'stopped': return '<span class="badge bg-warning text-dark">中断</span>';
        default: return `<span class="badge bg-dark">${status}</span>`;
    }
}

function openQuickDownload(tableName) {
    document.getElementById('modalTableName').value = tableName;
    document.getElementById('modalTitle').innerText = `下载数据 - ${tableName}`;

    const modalElement = document.getElementById('downloadModal');
    const modal = bootstrap.Modal.getOrCreateInstance(modalElement);
    modal.show();
}

async function confirmQuickDownload() {
    const tableName = document.getElementById('modalTableName').value;
    const monthsRaw = document.getElementById('modalMonths').value;
    const months = monthsRaw.split(',').map(m => m.trim()).filter(m => m);

    try {
        await startTask(tableName, null, months.length ? months : null);
        const modal = bootstrap.Modal.getInstance(document.getElementById('downloadModal'));
        if (modal) modal.hide();
        // Switch to task monitor tab
        const tabTrigger = new bootstrap.Tab(document.getElementById('task-monitor-tab'));
        tabTrigger.show();
    } catch (e) {
        alert('启动失败: ' + e.message);
    }
}

async function submitAdvancedTask() {
    const table_name = document.getElementById('raw_table_name').value;
    const symbols = document.getElementById('raw_symbols').value.split(',').map(s => s.trim()).filter(s => s);
    const months = document.getElementById('raw_months').value.split(',').map(m => m.trim()).filter(m => m);

    try {
        await startTask(table_name, symbols.length ? symbols : null, months.length ? months : null);
        // Switch to task monitor tab
        const tabTrigger = new bootstrap.Tab(document.getElementById('task-monitor-tab'));
        tabTrigger.show();
    } catch (e) {
        alert('提交失败: ' + e.message);
    }
}

async function startTask(table_name, symbols, months) {
    const result = await fetchApi('/market/tasks/download', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ table_name, symbols, months })
    });
    refreshTasks(); // Refresh immediately after starting
    return result;
}

document.addEventListener('DOMContentLoaded', () => {
    initDashboard();
    // Poll for updates every 3 seconds
    setInterval(() => {
        refreshTasks();
        // Only refresh metadata if we are on the audit tab
        const auditTab = document.getElementById('audit');
        if (auditTab && auditTab.classList.contains('active')) {
            initDashboard();
        }
    }, 3000);
});
