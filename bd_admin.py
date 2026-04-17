"""
Administrador de Base de Datos Web - VecinosMérida
Clon simplificado de phpMyAdmin para acceso rápido a MySQL
Ruta: /phpmyadmin
"""
from fastapi import APIRouter, HTTPException, Request, Depends
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from pydantic import BaseModel
import os
import io
import csv
import json as json_lib
from typing import Optional, List, Dict, Any
from db import get_conn

router = APIRouter(prefix="/phpmyadmin", tags=["Database Admin"])


# ═══════════════════════════════════════════════════════════════════════════
# MODELOS
# ═══════════════════════════════════════════════════════════════════════════

class SQLQuery(BaseModel):
    query: str
    export_format: Optional[str] = None  # 'csv', 'json', o None


# ═══════════════════════════════════════════════════════════════════════════
# AUTENTICACIÓN SIMPLE
# ═══════════════════════════════════════════════════════════════════════════

ADMIN_KEY = os.getenv("DB_ADMIN_KEY", "vecinosmerida2026")  # Cambiar en producción

def verify_admin_key(key: str):
    if key != ADMIN_KEY:
        raise HTTPException(status_code=403, detail="Clave de administrador incorrecta")
    return True


# ═══════════════════════════════════════════════════════════════════════════
# ENDPOINTS
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/", response_class=HTMLResponse)
async def db_admin_ui():
    """Interfaz principal del administrador de BD"""
    
    html = """
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>VecinosMérida - Administrador BD</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: linear-gradient(135deg, #1e3c72 0%, #2a5298 100%);
            min-height: 100vh;
            color: #2c3e50;
        }
        
        .header {
            background: rgba(255, 255, 255, 0.95);
            padding: 1.5rem 2rem;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        
        .header h1 {
            color: #1e3c72;
            font-size: 1.5rem;
        }
        
        .badge {
            background: #10b981;
            color: white;
            padding: 0.5rem 1rem;
            border-radius: 20px;
            font-size: 0.85rem;
            font-weight: 600;
        }
        
        .container {
            max-width: 1400px;
            margin: 2rem auto;
            padding: 0 2rem;
        }
        
        .card {
            background: white;
            border-radius: 12px;
            box-shadow: 0 4px 20px rgba(0,0,0,0.1);
            overflow: hidden;
            margin-bottom: 1.5rem;
        }
        
        .card-header {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 1.5rem;
            font-weight: 600;
            font-size: 1.1rem;
        }
        
        .card-body {
            padding: 1.5rem;
        }
        
        .auth-box {
            max-width: 500px;
            margin: 4rem auto;
        }
        
        .auth-input {
            width: 100%;
            padding: 1rem;
            border: 2px solid #e2e8f0;
            border-radius: 8px;
            font-size: 1rem;
            margin-bottom: 1rem;
            transition: border 0.3s;
        }
        
        .auth-input:focus {
            outline: none;
            border-color: #667eea;
        }
        
        .btn {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 1rem 2rem;
            border: none;
            border-radius: 8px;
            font-size: 1rem;
            font-weight: 600;
            cursor: pointer;
            width: 100%;
            transition: transform 0.2s, box-shadow 0.2s;
        }
        
        .btn:hover {
            transform: translateY(-2px);
            box-shadow: 0 6px 20px rgba(102, 126, 234, 0.4);
        }
        
        .btn:active {
            transform: translateY(0);
        }
        
        .btn-secondary {
            background: #94a3b8;
            margin-top: 0.5rem;
        }
        
        .btn-export {
            background: #10b981;
            padding: 0.75rem 1.5rem;
            display: inline-block;
            margin-right: 0.5rem;
            width: auto;
        }
        
        .grid {
            display: grid;
            grid-template-columns: 250px 1fr;
            gap: 1.5rem;
        }
        
        .sidebar {
            background: white;
            border-radius: 12px;
            padding: 1.5rem;
            box-shadow: 0 4px 20px rgba(0,0,0,0.1);
            height: fit-content;
        }
        
        .sidebar h3 {
            color: #1e3c72;
            margin-bottom: 1rem;
            font-size: 0.9rem;
            text-transform: uppercase;
            letter-spacing: 1px;
        }
        
        .table-list {
            list-style: none;
        }
        
        .table-item {
            padding: 0.75rem;
            margin-bottom: 0.5rem;
            background: #f8fafc;
            border-radius: 6px;
            cursor: pointer;
            transition: all 0.2s;
            font-size: 0.9rem;
        }
        
        .table-item:hover {
            background: #e0e7ff;
            transform: translateX(5px);
        }
        
        .table-item.active {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
        }
        
        #sqlEditor {
            width: 100%;
            min-height: 200px;
            padding: 1rem;
            border: 2px solid #e2e8f0;
            border-radius: 8px;
            font-family: 'Monaco', 'Menlo', monospace;
            font-size: 0.95rem;
            resize: vertical;
        }
        
        #sqlEditor:focus {
            outline: none;
            border-color: #667eea;
        }
        
        .results-container {
            margin-top: 2rem;
            max-height: 600px;
            overflow: auto;
        }
        
        table {
            width: 100%;
            border-collapse: collapse;
            font-size: 0.9rem;
        }
        
        th {
            background: #f1f5f9;
            padding: 1rem;
            text-align: left;
            font-weight: 600;
            position: sticky;
            top: 0;
            border-bottom: 2px solid #e2e8f0;
        }
        
        td {
            padding: 0.75rem 1rem;
            border-bottom: 1px solid #f1f5f9;
        }
        
        tr:hover {
            background: #fafbfc;
        }
        
        .stats {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 1rem;
            margin-bottom: 1.5rem;
        }
        
        .stat-card {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 1.5rem;
            border-radius: 8px;
            text-align: center;
        }
        
        .stat-value {
            font-size: 2rem;
            font-weight: bold;
            margin-bottom: 0.5rem;
        }
        
        .stat-label {
            font-size: 0.9rem;
            opacity: 0.9;
        }
        
        .error {
            background: #fee2e2;
            color: #991b1b;
            padding: 1rem;
            border-radius: 8px;
            border-left: 4px solid #dc2626;
        }
        
        .success {
            background: #d1fae5;
            color: #065f46;
            padding: 1rem;
            border-radius: 8px;
            border-left: 4px solid #10b981;
        }
        
        .hidden { display: none !important; }
        
        .loader {
            border: 3px solid #f3f4f6;
            border-top: 3px solid #667eea;
            border-radius: 50%;
            width: 40px;
            height: 40px;
            animation: spin 1s linear infinite;
            margin: 2rem auto;
        }
        
        @keyframes spin {
            0% { transform: rotate(0deg); }
            100% { transform: rotate(360deg); }
        }
    </style>
</head>
<body>
    <div class="header">
        <h1>🗄️ Administrador de Base de Datos</h1>
        <div class="badge" id="dbName">VecinosMérida MySQL</div>
    </div>

    <!-- Auth Screen -->
    <div id="authScreen">
        <div class="auth-box card">
            <div class="card-header">🔐 Autenticación requerida</div>
            <div class="card-body">
                <input type="password" id="adminKey" class="auth-input" placeholder="Clave de administrador" />
                <button class="btn" onclick="authenticate()">Acceder</button>
            </div>
        </div>
    </div>

    <!-- Main App -->
    <div id="mainApp" class="hidden">
        <div class="container">
            <div class="grid">
                <!-- Sidebar -->
                <div class="sidebar">
                    <h3>📊 Tablas</h3>
                    <ul class="table-list" id="tableList">
                        <li class="table-item">Cargando...</li>
                    </ul>
                </div>

                <!-- Main Content -->
                <div>
                    <!-- Stats -->
                    <div class="stats" id="statsContainer"></div>

                    <!-- SQL Editor -->
                    <div class="card">
                        <div class="card-header">✍️ Editor SQL</div>
                        <div class="card-body">
                            <textarea id="sqlEditor" placeholder="SELECT * FROM tabla LIMIT 100;"></textarea>
                            <button class="btn" onclick="executeQuery()">▶️ Ejecutar Query</button>
                            <button class="btn btn-export" onclick="exportResults('csv')">📥 Exportar CSV</button>
                            <button class="btn btn-export" onclick="exportResults('json')">📥 Exportar JSON</button>
                        </div>
                    </div>

                    <!-- Results -->
                    <div id="resultsContainer"></div>
                </div>
            </div>
        </div>
    </div>

    <script>
        let adminKey = '';
        let lastResults = null;

        function authenticate() {
            adminKey = document.getElementById('adminKey').value;
            if (!adminKey) {
                alert('Por favor ingresa la clave');
                return;
            }
            
            // Probar autenticación
            fetch('/BD/tables', {
                headers: { 'X-Admin-Key': adminKey }
            })
            .then(r => r.json())
            .then(data => {
                if (data.error) throw new Error(data.error);
                document.getElementById('authScreen').classList.add('hidden');
                document.getElementById('mainApp').classList.remove('hidden');
                loadTables();
            })
            .catch(err => {
                alert('Clave incorrecta: ' + err.message);
            });
        }

        async function loadTables() {
            try {
                const response = await fetch('/BD/tables', {
                    headers: { 'X-Admin-Key': adminKey }
                });
                const data = await response.json();
                
                const tableList = document.getElementById('tableList');
                tableList.innerHTML = data.tables.map(table => 
                    `<li class="table-item" onclick="loadTableData('${table}')">${table}</li>`
                ).join('');
                
                // Load stats
                loadStats();
            } catch (err) {
                console.error('Error loading tables:', err);
            }
        }

        async function loadStats() {
            try {
                const response = await fetch('/BD/stats', {
                    headers: { 'X-Admin-Key': adminKey }
                });
                const data = await response.json();
                
                const statsContainer = document.getElementById('statsContainer');
                statsContainer.innerHTML = Object.entries(data.table_counts).slice(0, 4).map(([table, count]) => `
                    <div class="stat-card">
                        <div class="stat-value">${count}</div>
                        <div class="stat-label">${table}</div>
                    </div>
                `).join('');
            } catch (err) {
                console.error('Error loading stats:', err);
            }
        }

        function loadTableData(tableName) {
            document.querySelectorAll('.table-item').forEach(el => el.classList.remove('active'));
            event.target.classList.add('active');
            
            document.getElementById('sqlEditor').value = `SELECT * FROM ${tableName} LIMIT 100;`;
            executeQuery();
        }

        async function executeQuery(exportFormat = null) {
            const query = document.getElementById('sqlEditor').value.trim();
            if (!query) {
                alert('Por favor escribe una query');
                return;
            }

            const resultsContainer = document.getElementById('resultsContainer');
            resultsContainer.innerHTML = '<div class="loader"></div>';

            try {
                const response = await fetch('/BD/execute', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'X-Admin-Key': adminKey
                    },
                    body: JSON.stringify({ query, export_format: exportFormat })
                });

                if (exportFormat) {
                    const blob = await response.blob();
                    const url = window.URL.createObjectURL(blob);
                    const a = document.createElement('a');
                    a.href = url;
                    a.download = `export_${Date.now()}.${exportFormat}`;
                    a.click();
                    resultsContainer.innerHTML = '<div class="success">✅ Exportación completada</div>';
                    return;
                }

                const data = await response.json();
                
                if (data.error) {
                    resultsContainer.innerHTML = `<div class="error">❌ ${data.error}</div>`;
                    return;
                }

                lastResults = data;
                displayResults(data);
            } catch (err) {
                resultsContainer.innerHTML = `<div class="error">❌ Error: ${err.message}</div>`;
            }
        }

        function displayResults(data) {
            const resultsContainer = document.getElementById('resultsContainer');
            
            if (data.type === 'select') {
                const table = `
                    <div class="card">
                        <div class="card-header">📋 Resultados (${data.row_count} filas)</div>
                        <div class="card-body">
                            <div class="results-container">
                                <table>
                                    <thead>
                                        <tr>${data.columns.map(col => `<th>${col}</th>`).join('')}</tr>
                                    </thead>
                                    <tbody>
                                        ${data.rows.map(row => `
                                            <tr>${data.columns.map(col => `<td>${row[col] !== null ? row[col] : '<i>NULL</i>'}</td>`).join('')}</tr>
                                        `).join('')}
                                    </tbody>
                                </table>
                            </div>
                        </div>
                    </div>
                `;
                resultsContainer.innerHTML = table;
            } else {
                resultsContainer.innerHTML = `
                    <div class="success">
                        ✅ Query ejecutado exitosamente<br>
                        Filas afectadas: ${data.rows_affected || 0}
                    </div>
                `;
                loadStats(); // Refresh stats
            }
        }

        function exportResults(format) {
            if (!lastResults || lastResults.type !== 'select') {
                alert('Primero ejecuta un SELECT para exportar');
                return;
            }
            executeQuery(format);
        }
    </script>
</body>
</html>
    """
    return html


@router.get("/tables")
async def get_tables(request: Request):
    """Lista todas las tablas de la base de datos"""
    admin_key = request.headers.get("X-Admin-Key")
    verify_admin_key(admin_key)
    
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("SHOW TABLES")
    tables = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    
    return {"tables": tables}


@router.get("/stats")
async def get_stats(request: Request):
    """Obtiene estadísticas de las tablas"""
    admin_key = request.headers.get("X-Admin-Key")
    verify_admin_key(admin_key)
    
    conn = get_conn()
    cursor = conn.cursor()
    
    cursor.execute("SHOW TABLES")
    tables = [row[0] for row in cursor.fetchall()]
    
    table_counts = {}
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        table_counts[table] = count
    
    cursor.close()
    conn.close()
    
    return {"table_counts": table_counts}


@router.post("/execute")
async def execute_query(query_data: SQLQuery, request: Request):
    """Ejecuta una query SQL"""
    admin_key = request.headers.get("X-Admin-Key")
    verify_admin_key(admin_key)
    
    query = query_data.query.strip()
    export_format = query_data.export_format
    
    # Validación básica de seguridad
    dangerous_keywords = ["DROP DATABASE", "DROP TABLE", "TRUNCATE"]
    if any(kw in query.upper() for kw in dangerous_keywords):
        raise HTTPException(status_code=403, detail="Query peligrosa bloqueada")
    
    conn = get_conn()
    cursor = conn.cursor(dictionary=True)
    
    try:
        cursor.execute(query)
        
        # Detectar tipo de query
        is_select = query.strip().upper().startswith("SELECT")
        
        if is_select:
            rows = cursor.fetchall()
            columns = list(rows[0].keys()) if rows else []
            
            # Si es exportación
            if export_format == 'csv':
                output = io.StringIO()
                writer = csv.DictWriter(output, fieldnames=columns)
                writer.writeheader()
                writer.writerows(rows)
                output.seek(0)
                return StreamingResponse(
                    io.BytesIO(output.getvalue().encode()),
                    media_type="text/csv",
                    headers={"Content-Disposition": f"attachment; filename=export_{int(os.time())}.csv"}
                )
            
            if export_format == 'json':
                json_data = json_lib.dumps(rows, ensure_ascii=False, indent=2, default=str)
                return StreamingResponse(
                    io.BytesIO(json_data.encode()),
                    media_type="application/json",
                    headers={"Content-Disposition": f"attachment; filename=export_{int(os.time())}.json"}
                )
            
            return {
                "type": "select",
                "columns": columns,
                "rows": rows,
                "row_count": len(rows)
            }
        else:
            conn.commit()
            return {
                "type": "modify",
                "rows_affected": cursor.rowcount,
                "message": "Query ejecutado exitosamente"
            }
    
    except Exception as e:
        conn.rollback()
        return {"error": str(e)}
    
    finally:
        cursor.close()
        conn.close()
