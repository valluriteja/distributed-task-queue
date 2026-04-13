import { useState, useEffect } from "react";
import axios from "axios";

const API = "http://127.0.0.1:8000";

export default function App() {
  const [stats, setStats] = useState({
    high_priority_queue: 0,
    normal_queue: 0,
    dead_letter_queue: 0,
    total_tasks_processed: 0,
    avg_processing_time_seconds: 0,
  });
  const [deadTasks, setDeadTasks] = useState([]);
  const [taskType, setTaskType] = useState("send_email");
  const [priority, setPriority] = useState(1);
  const [message, setMessage] = useState("");
  const [trackId, setTrackId] = useState("");
  const [trackedTask, setTrackedTask] = useState(null);

  useEffect(() => {
    fetchStats();
    const interval = setInterval(fetchStats, 3000);
    return () => clearInterval(interval);
  }, []);

  const fetchStats = async () => {
    try {
      const res = await axios.get(`${API}/stats`);
      setStats(res.data);
      const dead = await axios.get(`${API}/dead-letter`);
      setDeadTasks(dead.data.failed_tasks);
    } catch (err) {
      console.error("API not reachable");
    }
  };

  const submitTask = async () => {
    try {
      const res = await axios.post(`${API}/tasks`, {
        task_type: taskType,
        payload: { to: "test@gmail.com", filename: "photo.jpg" },
        priority: parseInt(priority),
      });
      setMessage(`✅ Task submitted! ID: ${res.data.task_id}`);
      setTrackId(res.data.task_id);
      setTimeout(() => setMessage(""), 5000);
      fetchStats();
    } catch (err) {
      setMessage("❌ Failed to submit task!");
    }
  };

  const trackTask = async () => {
    try {
      const res = await axios.get(`${API}/tasks/${trackId}`);
      setTrackedTask(res.data);
    } catch (err) {
      setTrackedTask({ status: "not found" });
    }
  };

  const statusColor = (status) => {
    if (status === "completed") return "#00ff88";
    if (status === "failed") return "#ff4444";
    if (status === "processing") return "#ffaa00";
    return "#888";
  };

  return (
    <div style={{ fontFamily: "monospace", padding: "30px", backgroundColor: "#0f0f0f", minHeight: "100vh", color: "white" }}>

      <h1 style={{ color: "#00ff88" }}>⚡ Task Queue Dashboard</h1>
      <p style={{ color: "#888" }}>Auto-refreshes every 3 seconds</p>

      {/* Stats Cards */}
      <div style={{ display: "flex", gap: "20px", marginTop: "30px", flexWrap: "wrap" }}>
        <StatCard label="🔴 High Priority" value={stats.high_priority_queue} color="#ff4444" />
        <StatCard label="🟢 Normal Queue" value={stats.normal_queue} color="#00ff88" />
        <StatCard label="💀 Dead Letter" value={stats.dead_letter_queue} color="#ff8800" />
        <StatCard label="✅ Total Processed" value={stats.total_tasks_processed} color="#00aaff" />
        <StatCard label="⏱️ Avg Time (s)" value={stats.avg_processing_time_seconds} color="#aa00ff" />
      </div>

      {/* Submit Task */}
      <div style={{ marginTop: "40px", backgroundColor: "#1a1a1a", padding: "20px", borderRadius: "10px" }}>
        <h2 style={{ color: "#00ff88" }}>Submit a Task</h2>
        <div style={{ display: "flex", gap: "10px", alignItems: "center", flexWrap: "wrap" }}>
          <select value={taskType} onChange={e => setTaskType(e.target.value)}
            style={{ padding: "10px", borderRadius: "6px", backgroundColor: "#333", color: "white", border: "none" }}>
            <option value="send_email">send_email</option>
            <option value="resize_image">resize_image</option>
            <option value="failing_task">failing_task</option>
          </select>

          <select value={priority} onChange={e => setPriority(e.target.value)}
            style={{ padding: "10px", borderRadius: "6px", backgroundColor: "#333", color: "white", border: "none" }}>
            <option value={1}>🟢 Normal Priority</option>
            <option value={2}>🔴 High Priority</option>
          </select>

          <button onClick={submitTask}
            style={{ padding: "10px 20px", backgroundColor: "#00ff88", color: "#000", border: "none", borderRadius: "6px", cursor: "pointer", fontWeight: "bold" }}>
            Submit Task
          </button>

          {message && <span style={{ color: "#00ff88", fontSize: "12px" }}>{message}</span>}
        </div>
      </div>

      {/* Task Status Tracker */}
      <div style={{ marginTop: "30px", backgroundColor: "#1a1a1a", padding: "20px", borderRadius: "10px" }}>
        <h2 style={{ color: "#00aaff" }}>🔍 Track a Task</h2>
        <div style={{ display: "flex", gap: "10px", alignItems: "center", flexWrap: "wrap" }}>
          <input
            value={trackId}
            onChange={e => setTrackId(e.target.value)}
            placeholder="Paste task ID here..."
            style={{ padding: "10px", borderRadius: "6px", backgroundColor: "#333", color: "white", border: "none", width: "320px" }}
          />
          <button onClick={trackTask}
            style={{ padding: "10px 20px", backgroundColor: "#00aaff", color: "#000", border: "none", borderRadius: "6px", cursor: "pointer", fontWeight: "bold" }}>
            Track
          </button>
        </div>

        {trackedTask && (
          <div style={{ marginTop: "15px", padding: "15px", backgroundColor: "#222", borderRadius: "8px", borderLeft: `4px solid ${statusColor(trackedTask.status)}` }}>
            <p style={{ margin: "5px 0" }}>Status: <span style={{ color: statusColor(trackedTask.status), fontWeight: "bold" }}>{trackedTask.status?.toUpperCase()}</span></p>
            <p style={{ margin: "5px 0", color: "#888" }}>Type: {trackedTask.type}</p>
            <p style={{ margin: "5px 0", color: "#888" }}>Created: {trackedTask.created_at}</p>
            {trackedTask.updated_at && <p style={{ margin: "5px 0", color: "#888" }}>Updated: {trackedTask.updated_at}</p>}
            {trackedTask.error && <p style={{ margin: "5px 0", color: "#ff4444" }}>Error: {trackedTask.error}</p>}
          </div>
        )}
      </div>

      {/* Dead Letter Queue */}
      <div style={{ marginTop: "30px", backgroundColor: "#1a1a1a", padding: "20px", borderRadius: "10px" }}>
        <h2 style={{ color: "#ff8800" }}>💀 Failed Tasks (Dead Letter Queue)</h2>
        {deadTasks.length === 0 ? (
          <p style={{ color: "#888" }}>No failed tasks 🎉</p>
        ) : (
          <table style={{ width: "100%", borderCollapse: "collapse" }}>
            <thead>
              <tr style={{ color: "#888", textAlign: "left" }}>
                <th style={{ padding: "10px" }}>ID</th>
                <th style={{ padding: "10px" }}>Type</th>
                <th style={{ padding: "10px" }}>Retries</th>
                <th style={{ padding: "10px" }}>Failed At</th>
              </tr>
            </thead>
            <tbody>
              {deadTasks.map(task => (
                <tr key={task.id} style={{ borderTop: "1px solid #333" }}>
                  <td style={{ padding: "10px", color: "#ff4444" }}>{task.id.slice(0, 8)}...</td>
                  <td style={{ padding: "10px" }}>{task.type}</td>
                  <td style={{ padding: "10px" }}>{task.retries}</td>
                  <td style={{ padding: "10px", color: "#888" }}>{task.failed_at}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}

function StatCard({ label, value, color }) {
  return (
    <div style={{ backgroundColor: "#1a1a1a", padding: "20px 30px", borderRadius: "10px", borderLeft: `4px solid ${color}`, minWidth: "150px" }}>
      <p style={{ color: "#888", margin: 0 }}>{label}</p>
      <h2 style={{ color, margin: "5px 0 0 0", fontSize: "2rem" }}>{value}</h2>
    </div>
  );
}