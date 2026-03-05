import React, { useEffect, useState } from 'react';
import {
  Play, RefreshCw, CheckCircle2, XCircle, Clock,
  Loader2, Zap, AlertCircle,
} from 'lucide-react';

interface JobRun {
  run_id: number;
  state: string;
  result_state: string | null;
  start_time: number;
  end_time: number | null;
  duration_ms: number | null;
  trigger: string;
}

function StatusBadge({ state, resultState }: { state: string; resultState: string | null }) {
  if (state === 'TERMINATED' && resultState === 'SUCCESS') {
    return (
      <span className="inline-flex items-center gap-1 px-2.5 py-1 rounded-full text-xs font-medium bg-green-50 text-green-700 border border-green-200">
        <CheckCircle2 size={12} /> Success
      </span>
    );
  }
  if (state === 'TERMINATED' && resultState === 'FAILED') {
    return (
      <span className="inline-flex items-center gap-1 px-2.5 py-1 rounded-full text-xs font-medium bg-red-50 text-red-700 border border-red-200">
        <XCircle size={12} /> Failed
      </span>
    );
  }
  if (state === 'RUNNING') {
    return (
      <span className="inline-flex items-center gap-1 px-2.5 py-1 rounded-full text-xs font-medium bg-blue-50 text-blue-700 border border-blue-200">
        <Loader2 size={12} className="animate-spin" /> Running
      </span>
    );
  }
  if (state === 'PENDING') {
    return (
      <span className="inline-flex items-center gap-1 px-2.5 py-1 rounded-full text-xs font-medium bg-yellow-50 text-yellow-700 border border-yellow-200">
        <Clock size={12} /> Pending
      </span>
    );
  }
  return (
    <span className="inline-flex items-center gap-1 px-2.5 py-1 rounded-full text-xs font-medium bg-gray-50 text-gray-600 border border-gray-200">
      {state} {resultState ? `/ ${resultState}` : ''}
    </span>
  );
}

function formatDuration(ms: number | null) {
  if (!ms) return '--';
  const seconds = Math.floor(ms / 1000);
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.floor(seconds / 60);
  const remainingSec = seconds % 60;
  return `${minutes}m ${remainingSec}s`;
}

function formatTime(timestamp: number | null) {
  if (!timestamp) return '--';
  return new Date(timestamp).toLocaleString('en-US', {
    month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit',
  });
}

export default function UploadProcess() {
  const [runs, setRuns] = useState<JobRun[]>([]);
  const [loading, setLoading] = useState(true);
  const [triggering, setTriggering] = useState(false);
  const [triggerResult, setTriggerResult] = useState<{ success: boolean; message: string } | null>(null);
  const [error, setError] = useState<string | null>(null);

  const fetchRuns = async () => {
    setLoading(true);
    try {
      const res = await fetch('/api/job-runs');
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const json = await res.json();
      setRuns(json);
      setError(null);
    } catch (err: any) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const handleTrigger = async () => {
    setTriggering(true);
    setTriggerResult(null);
    try {
      const res = await fetch('/api/trigger-pipeline', { method: 'POST' });
      const json = await res.json();
      if (!res.ok) throw new Error(json.error || 'Failed to trigger pipeline');
      setTriggerResult({ success: true, message: `Pipeline triggered (Run ID: ${json.run_id})` });
      setTimeout(fetchRuns, 2000);
    } catch (err: any) {
      setTriggerResult({ success: false, message: err.message });
    } finally {
      setTriggering(false);
    }
  };

  useEffect(() => { fetchRuns(); }, []);

  return (
    <div className="space-y-6">
      {/* Pipeline Control */}
      <div className="bg-white rounded-xl shadow-sm border border-gray-100 p-6">
        <div className="flex items-center justify-between">
          <div>
            <h2 className="text-lg font-semibold text-navy-800">Supplier Processing Pipeline</h2>
            <p className="text-sm text-gray-500 mt-1">
              Trigger the full pipeline: ingest raw supplier data, run AI SQL cleaning, detect anomalies, and write to gold tables.
            </p>
          </div>
          <div className="flex items-center gap-3">
            <button
              onClick={fetchRuns}
              disabled={loading}
              className="p-2.5 rounded-lg border border-gray-200 text-gray-500 hover:bg-gray-50 transition-colors disabled:opacity-50"
              title="Refresh"
            >
              <RefreshCw size={16} className={loading ? 'animate-spin' : ''} />
            </button>
            <button
              onClick={handleTrigger}
              disabled={triggering}
              className="inline-flex items-center gap-2 px-5 py-2.5 bg-navy-800 text-white rounded-lg font-medium text-sm hover:bg-navy-700 transition-colors disabled:opacity-60 shadow-sm"
            >
              {triggering ? (
                <Loader2 size={16} className="animate-spin" />
              ) : (
                <Play size={16} />
              )}
              {triggering ? 'Triggering...' : 'Run Pipeline'}
            </button>
          </div>
        </div>

        {triggerResult && (
          <div className={`mt-4 flex items-center gap-2 px-4 py-3 rounded-lg text-sm ${
            triggerResult.success
              ? 'bg-green-50 text-green-700 border border-green-200'
              : 'bg-red-50 text-red-700 border border-red-200'
          }`}>
            {triggerResult.success ? <Zap size={16} /> : <AlertCircle size={16} />}
            {triggerResult.message}
          </div>
        )}
      </div>

      {/* Pipeline Steps */}
      <div className="bg-white rounded-xl shadow-sm border border-gray-100 p-6">
        <h3 className="text-sm font-semibold text-navy-800 uppercase tracking-wider mb-4">Pipeline Steps</h3>
        <div className="flex items-center gap-2 overflow-x-auto pb-2">
          {[
            { step: '1', label: 'Ingest Raw Data', desc: '500 bilingual suppliers' },
            { step: '2', label: 'AI SQL Cleaning', desc: 'ai_query() standardization' },
            { step: '3', label: 'Duplicate Detection', desc: 'JP/EN cross-language' },
            { step: '4', label: 'Anomaly Detection', desc: 'Price, lead time, certs' },
            { step: '5', label: 'Gold + Escalation', desc: '118 approved, 382 flagged' },
          ].map((item, i) => (
            <React.Fragment key={i}>
              {i > 0 && (
                <div className="flex-shrink-0 w-8 h-0.5 bg-gold-400" />
              )}
              <div className="flex-shrink-0 bg-navy-50 border border-navy-200 rounded-lg px-4 py-3 text-center min-w-[150px]">
                <div className="w-7 h-7 rounded-full bg-navy-800 text-white text-xs font-bold flex items-center justify-center mx-auto mb-1.5">
                  {item.step}
                </div>
                <p className="text-sm font-medium text-navy-800">{item.label}</p>
                <p className="text-xs text-gray-500">{item.desc}</p>
              </div>
            </React.Fragment>
          ))}
        </div>
      </div>

      {/* Recent Runs Table */}
      <div className="bg-white rounded-xl shadow-sm border border-gray-100 overflow-hidden">
        <div className="px-6 py-4 border-b border-gray-100">
          <h3 className="text-sm font-semibold text-navy-800 uppercase tracking-wider">Recent Pipeline Runs</h3>
        </div>

        {error ? (
          <div className="p-6 text-center">
            <AlertCircle className="mx-auto text-warmred-500 mb-2" size={24} />
            <p className="text-sm text-red-600">{error}</p>
          </div>
        ) : loading ? (
          <div className="p-8 text-center text-gray-500">
            <Loader2 className="animate-spin mx-auto mb-2" size={24} />
            Loading runs...
          </div>
        ) : runs.length === 0 ? (
          <div className="p-8 text-center text-gray-400">
            <Clock className="mx-auto mb-2" size={32} />
            <p>No pipeline runs found.</p>
            <p className="text-sm mt-1">Click "Run Pipeline" to start.</p>
          </div>
        ) : (
          <table className="w-full text-sm">
            <thead>
              <tr className="bg-gray-50 text-left">
                <th className="px-6 py-3 font-medium text-gray-500">Run ID</th>
                <th className="px-6 py-3 font-medium text-gray-500">Status</th>
                <th className="px-6 py-3 font-medium text-gray-500">Started</th>
                <th className="px-6 py-3 font-medium text-gray-500">Duration</th>
                <th className="px-6 py-3 font-medium text-gray-500">Trigger</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {runs.map((run) => (
                <tr key={run.run_id} className="hover:bg-gray-50 transition-colors">
                  <td className="px-6 py-3 font-mono text-xs text-navy-700">{run.run_id}</td>
                  <td className="px-6 py-3">
                    <StatusBadge state={run.state} resultState={run.result_state} />
                  </td>
                  <td className="px-6 py-3 text-gray-600">{formatTime(run.start_time)}</td>
                  <td className="px-6 py-3 text-gray-600">{formatDuration(run.duration_ms)}</td>
                  <td className="px-6 py-3 text-gray-500 capitalize">{String(run.trigger).toLowerCase()}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}
