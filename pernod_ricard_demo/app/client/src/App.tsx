import React, { useState } from 'react';
import Dashboard from './components/Dashboard';
import UploadProcess from './components/UploadProcess';
import KnowledgeAssistant from './components/KnowledgeAssistant';
import ReviewQueue from './components/ReviewQueue';
import {
  LayoutDashboard,
  Upload,
  MessageSquare,
  ClipboardCheck,
} from 'lucide-react';

const tabs = [
  { id: 'dashboard', label: 'Dashboard', icon: LayoutDashboard },
  { id: 'upload', label: 'Upload & Process', icon: Upload },
  { id: 'assistant', label: 'Knowledge Assistant', icon: MessageSquare },
  { id: 'review', label: 'Review Queue', icon: ClipboardCheck },
] as const;

type TabId = (typeof tabs)[number]['id'];

export default function App() {
  const [activeTab, setActiveTab] = useState<TabId>('dashboard');

  return (
    <div className="min-h-screen bg-gray-50 flex flex-col">
      {/* Header */}
      <header className="bg-navy-800 text-white shadow-lg">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex items-center justify-between h-16">
            <div className="flex items-center gap-4">
              <div className="flex items-center gap-3">
                <div className="w-9 h-9 rounded bg-gold-400 flex items-center justify-center">
                  <span className="text-navy-800 font-bold text-sm">PR</span>
                </div>
                <div>
                  <h1 className="text-lg font-semibold leading-tight tracking-tight">
                    Pernod Ricard Japan
                  </h1>
                  <p className="text-xs text-gold-400 font-medium tracking-wider uppercase">
                    Supplier Intelligence Hub
                  </p>
                </div>
              </div>
            </div>
            <div className="text-xs text-navy-300">
              Powered by Databricks
            </div>
          </div>
        </div>
      </header>

      {/* Tab Navigation */}
      <nav className="bg-white border-b border-gray-200 shadow-sm">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex space-x-1">
            {tabs.map((tab) => {
              const Icon = tab.icon;
              const isActive = activeTab === tab.id;
              return (
                <button
                  key={tab.id}
                  onClick={() => setActiveTab(tab.id)}
                  className={`
                    flex items-center gap-2 px-4 py-3 text-sm font-medium border-b-2 transition-colors
                    ${isActive
                      ? 'border-gold-400 text-navy-800'
                      : 'border-transparent text-gray-500 hover:text-navy-700 hover:border-gray-300'
                    }
                  `}
                >
                  <Icon size={16} />
                  {tab.label}
                </button>
              );
            })}
          </div>
        </div>
      </nav>

      {/* Content */}
      <main className="flex-1 max-w-7xl w-full mx-auto px-4 sm:px-6 lg:px-8 py-6">
        {activeTab === 'dashboard' && <Dashboard />}
        {activeTab === 'upload' && <UploadProcess />}
        {activeTab === 'assistant' && <KnowledgeAssistant />}
        {activeTab === 'review' && <ReviewQueue />}
      </main>

      {/* Footer */}
      <footer className="bg-navy-800 text-navy-300 text-xs py-3">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 flex justify-between">
          <span>Pernod Ricard Japan -- Supplier Intelligence Hub</span>
          <span>Data powered by Databricks Lakehouse</span>
        </div>
      </footer>
    </div>
  );
}
