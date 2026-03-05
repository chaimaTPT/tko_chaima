import React, { useState, useRef, useEffect } from 'react';
import { Send, Bot, User, Loader2, Sparkles, Trash2, BookOpen } from 'lucide-react';

interface Message {
  role: 'user' | 'assistant';
  content: string;
  sources?: string[];
}

const SUGGESTIONS = [
  'What certifications are mandatory for food-contact suppliers?',
  'What are the acceptable price ranges for glass bottles?',
  'What is the audit frequency for Tier 1 suppliers?',
  'What are the Japan-specific compliance requirements?',
  'Explain the supplier risk assessment scoring criteria.',
  'What lead time standards apply to packaging suppliers?',
];

export default function KnowledgeAssistant() {
  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState('');
  const [loading, setLoading] = useState(false);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLTextAreaElement>(null);

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages]);

  const sendMessage = async (content: string) => {
    if (!content.trim() || loading) return;

    const userMessage: Message = { role: 'user', content: content.trim() };
    const updatedMessages = [...messages, userMessage];
    setMessages(updatedMessages);
    setInput('');
    setLoading(true);

    try {
      const res = await fetch('/api/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          messages: updatedMessages.map(m => ({ role: m.role, content: m.content })),
        }),
      });

      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      setMessages([
        ...updatedMessages,
        { role: 'assistant', content: data.content, sources: data.sources },
      ]);
    } catch (err: any) {
      setMessages([
        ...updatedMessages,
        { role: 'assistant', content: `Sorry, I encountered an error: ${err.message}. Please try again.` },
      ]);
    } finally {
      setLoading(false);
      inputRef.current?.focus();
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      sendMessage(input);
    }
  };

  const clearChat = () => {
    setMessages([]);
  };

  return (
    <div className="flex flex-col h-[calc(100vh-220px)] min-h-[500px]">
      {/* Chat Header */}
      <div className="bg-white rounded-t-xl border border-gray-100 shadow-sm px-6 py-4 flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className="w-9 h-9 rounded-lg bg-gradient-to-br from-navy-800 to-navy-600 flex items-center justify-center">
            <Sparkles size={18} className="text-gold-400" />
          </div>
          <div>
            <h2 className="text-sm font-semibold text-navy-800">Procurement Knowledge Assistant</h2>
            <p className="text-xs text-gray-500">RAG-powered answers from procurement policy documents</p>
          </div>
        </div>
        <div className="flex items-center gap-2">
          <span className="inline-flex items-center gap-1 px-2 py-1 rounded-full text-xs font-medium bg-green-50 text-green-700 border border-green-200">
            <span className="w-1.5 h-1.5 rounded-full bg-green-500" />
            Online
          </span>
          {messages.length > 0 && (
            <button
              onClick={clearChat}
              className="text-gray-400 hover:text-gray-600 p-2 rounded-lg hover:bg-gray-100 transition-colors"
              title="Clear chat"
            >
              <Trash2 size={16} />
            </button>
          )}
        </div>
      </div>

      {/* Messages Area */}
      <div className="flex-1 bg-gray-50 border-x border-gray-100 overflow-y-auto px-6 py-4 space-y-4">
        {messages.length === 0 ? (
          <div className="flex flex-col items-center justify-center h-full text-center">
            <div className="w-16 h-16 rounded-2xl bg-navy-800 flex items-center justify-center mb-4">
              <Bot size={32} className="text-gold-400" />
            </div>
            <h3 className="text-lg font-semibold text-navy-800 mb-1">How can I help you?</h3>
            <p className="text-sm text-gray-500 mb-2 max-w-md">
              I answer questions grounded in Pernod Ricard Japan's procurement policies, compliance requirements, and supplier scoring criteria.
            </p>
            <p className="text-xs text-gray-400 mb-6 max-w-md">
              Powered by Databricks Agent Bricks Knowledge Assistant with Vector Search RAG over 3 policy documents.
            </p>
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-2 max-w-xl w-full">
              {SUGGESTIONS.map((suggestion, i) => (
                <button
                  key={i}
                  onClick={() => sendMessage(suggestion)}
                  className="text-left text-sm px-4 py-3 rounded-lg border border-gray-200 bg-white text-gray-700 hover:border-gold-400 hover:bg-gold-50 transition-colors"
                >
                  {suggestion}
                </button>
              ))}
            </div>
          </div>
        ) : (
          messages.map((msg, i) => (
            <div
              key={i}
              className={`flex gap-3 animate-fade-in-up ${msg.role === 'user' ? 'justify-end' : 'justify-start'}`}
            >
              {msg.role === 'assistant' && (
                <div className="flex-shrink-0 w-8 h-8 rounded-lg bg-navy-800 flex items-center justify-center mt-0.5">
                  <Bot size={16} className="text-gold-400" />
                </div>
              )}
              <div className={`max-w-[75%] ${msg.role === 'user' ? '' : ''}`}>
                <div
                  className={`rounded-2xl px-4 py-3 text-sm leading-relaxed ${
                    msg.role === 'user'
                      ? 'bg-navy-800 text-white rounded-br-md'
                      : 'bg-white border border-gray-200 text-gray-800 rounded-bl-md shadow-sm'
                  }`}
                >
                  <div className="whitespace-pre-wrap">{msg.content}</div>
                </div>
                {/* Source citations */}
                {msg.sources && msg.sources.length > 0 && (
                  <div className="mt-1.5 flex items-center gap-1.5 flex-wrap">
                    <BookOpen size={12} className="text-gray-400" />
                    {msg.sources.map((src, j) => (
                      <span
                        key={j}
                        className="inline-flex items-center px-2 py-0.5 rounded text-xs bg-navy-50 text-navy-600 border border-navy-100"
                      >
                        {src}
                      </span>
                    ))}
                  </div>
                )}
              </div>
              {msg.role === 'user' && (
                <div className="flex-shrink-0 w-8 h-8 rounded-lg bg-gold-400 flex items-center justify-center mt-0.5">
                  <User size={16} className="text-navy-800" />
                </div>
              )}
            </div>
          ))
        )}

        {loading && (
          <div className="flex gap-3 animate-fade-in-up">
            <div className="flex-shrink-0 w-8 h-8 rounded-lg bg-navy-800 flex items-center justify-center mt-0.5">
              <Bot size={16} className="text-gold-400" />
            </div>
            <div className="bg-white border border-gray-200 rounded-2xl rounded-bl-md px-4 py-3 shadow-sm">
              <div className="flex items-center gap-1.5">
                <div className="w-2 h-2 rounded-full bg-navy-400 pulse-dot" />
                <div className="w-2 h-2 rounded-full bg-navy-400 pulse-dot" />
                <div className="w-2 h-2 rounded-full bg-navy-400 pulse-dot" />
              </div>
            </div>
          </div>
        )}

        <div ref={messagesEndRef} />
      </div>

      {/* Input Area */}
      <div className="bg-white rounded-b-xl border border-gray-100 shadow-sm px-4 py-3">
        <div className="flex items-end gap-3">
          <textarea
            ref={inputRef}
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder="Ask about supplier policies, certifications, compliance..."
            rows={1}
            className="flex-1 resize-none rounded-lg border border-gray-200 px-4 py-2.5 text-sm text-gray-800 placeholder:text-gray-400 focus:outline-none focus:ring-2 focus:ring-gold-400 focus:border-transparent"
            style={{ minHeight: '42px', maxHeight: '120px' }}
            onInput={(e) => {
              const target = e.target as HTMLTextAreaElement;
              target.style.height = 'auto';
              target.style.height = Math.min(target.scrollHeight, 120) + 'px';
            }}
          />
          <button
            onClick={() => sendMessage(input)}
            disabled={!input.trim() || loading}
            className="flex-shrink-0 w-10 h-10 rounded-lg bg-navy-800 text-white flex items-center justify-center hover:bg-navy-700 transition-colors disabled:opacity-40 disabled:cursor-not-allowed"
          >
            {loading ? <Loader2 size={16} className="animate-spin" /> : <Send size={16} />}
          </button>
        </div>
      </div>
    </div>
  );
}
