import configparser
import concurrent.futures
import json
import os
import re
import sys
import traceback
import warnings
import webbrowser
from datetime import datetime
from pathlib import Path
from time import time
from typing import List, Optional, Dict, Any
import argparse

import mysql.connector
import psycopg2
import snowflake.connector
import pandas as pd
from colorama import Fore, Style, init
from tqdm import tqdm
from openpyxl import load_workbook

try:
    from jinja2 import Environment
except ImportError:
    Environment = None

init(autoreset=True)


FULL_REPORT_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Data Quality {{ report_domain }} Report</title>
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0-beta3/css/all.min.css">
    <script src="https://cdn.jsdelivr.net/npm/chart.js@3.9.1/dist/chart.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-datalabels@2.0.0"></script>
    <script src="https://cdn.jsdelivr.net/npm/chartjs-chart-treemap@2.0.2/dist/chartjs-chart-treemap.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/chartjs-chart-sankey@0.12.0/dist/chartjs-chart-sankey.min.js"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/html2pdf.js/0.10.1/html2pdf.bundle.min.js" integrity="sha512-GsLlZN/3F2ErC5ifS5QtgpiJtWd43JWSuIgh7mbzZ8zBps+dvLusV+eNQATqgA/HdeKFVgA5v3S/cIrLF7QnIg==" crossorigin="anonymous" referrerpolicy="no-referrer"></script>
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Nunito:wght@400;600;700;800&display=swap');
        :root { --primary-color: #36a2eb; --success-color: #28a745; --danger-color: #dc3545; --warning-color: #ffc107; --info-color: #17a2b8; --bg-color: #f4f7fa; --card-bg-color: #ffffff; --sidebar-bg-color: #002060; --sidebar-text-color: #e0e0e0; --text-color: #343a40; --subtle-text-color: #6c757d; --border-color: #e9ecef; --header-bg-color: #f8f9fa; }
        body.dark-mode { --bg-color: #121212; --card-bg-color: #1e1e1e; --sidebar-bg-color: #1e1e1e; --sidebar-text-color: #e0e0e0; --text-color: #e0e0e0; --subtle-text-color: #a0a0a0; --border-color: #333333; --header-bg-color: #2c2c2c; }
        body.dark-mode .sidebar-footer img { filter: invert(1) brightness(1.5); }
        html { font-size: 100%; scroll-behavior: smooth; }
        body { font-family: 'Nunito', sans-serif; background-color: var(--bg-color); color: var(--text-color); margin: 0; display: flex; overflow: hidden; transition: background-color 0.3s, color 0.3s; }
        .sidebar { width: 250px; background: var(--sidebar-bg-color); box-shadow: 0 0 15px rgba(0,0,0,0.05); display: flex; flex-direction: column; height: 100vh; position: fixed; z-index: 100; border-right: 1px solid var(--border-color); transition: transform 0.3s ease-in-out, width 0.3s ease-in-out; }
        .sidebar-header { padding: 20px; text-align: center; border-bottom: 1px solid var(--border-color); }
        .sidebar-header h2 { margin: 0; color: #ffffff; font-weight: 700; }
        .sidebar-nav { list-style: none; padding: 20px 0; margin: 0; }
        .sidebar-nav li .tab-button { display: flex; align-items: center; padding: 15px 25px; color: var(--sidebar-text-color); text-decoration: none; transition: all 0.3s ease; cursor: pointer; background: none; border: none; width: 100%; text-align: left; font-family: 'Nunito', sans-serif; font-size: 1.05em; font-weight: 600; }
        .sidebar-nav li .tab-button.active, .sidebar-nav li .tab-button:hover { background-color: rgba(255, 255, 255, 0.1); color: var(--primary-color); border-right: 3px solid var(--primary-color); }
        .sidebar-nav li .tab-button i { margin-right: 15px; width: 20px; text-align: center; }
        .sidebar-footer { margin-top: auto; padding: 20px; text-align: center; }
        .sidebar-footer img { max-width: 80%; height: auto; opacity: 0.8; transition: opacity 0.3s ease; }
        .sidebar-footer img:hover { opacity: 1; }
        .sidebar-resize-handle { width: 5px; height: 100%; background: var(--border-color); position: absolute; top: 0; right: 0; cursor: col-resize; z-index: 101; transition: background-color 0.1s; }
        .sidebar-resize-handle:hover { background-color: var(--primary-color); }
        .main-content { margin-left: 250px; flex-grow: 1; padding: 30px; transition: margin-left 0.3s ease-in-out, padding-left 0.3s ease-in-out; position: relative; overflow-y: auto; height: 100vh; }
        .header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; position: relative; }
        .header h1 { margin: 0 auto; font-size: 2em; font-weight: 800; }
        .header .timestamp { position: absolute; right: 0; top: 50%; transform: translateY(-50%); font-size: 0.9em; color: var(--subtle-text-color); }
        .page-layout { display: flex; flex-direction: column; gap: 20px; width: 100%; box-sizing: border-box; }
        .section { background: var(--card-bg-color); padding: 30px; border-radius: 8px; border: 1px solid var(--border-color); margin-bottom: 20px; overflow: hidden; }
        .section:last-child { margin-bottom: 0; }
        .section-title { margin: -30px -30px 25px -30px; padding: 20px 30px; border-bottom: 1px solid var(--border-color); font-size: 1.2em; font-weight: 700; display: flex; justify-content: space-between; align-items: center; }
        .dashboard-overview { display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 30px; }
        .dashboard-main-grid { display: grid; grid-template-columns: 1fr; gap: 30px; margin-bottom: 30px; }
        @media (min-width: 1200px) { .dashboard-main-grid { grid-template-columns: 3fr 2fr; } }
        .overview-chart-card { background: var(--card-bg-color); padding: 30px; border-radius: 8px; border: 1px solid var(--border-color); }
        .overview-chart-card .section-title { margin: -30px -30px 15px -30px; padding: 20px 30px; border-bottom: 1px solid var(--border-color); font-size: 1.2em; font-weight: 700; }
        .stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 25px; }
        @media (min-width: 768px) { .stats-grid { grid-template-columns: repeat(4, 1fr); } }
        .stat-card { background: var(--card-bg-color); padding: 25px; border-radius: 8px; display: flex; align-items: center; border: 1px solid var(--border-color); transition: all 0.3s ease-in-out; cursor: pointer; }
        .stat-card:hover { transform: translateY(-8px) scale(1.02); box-shadow: 0 12px 25px rgba(0,0,0,0.1); }
        .stat-card .icon { font-size: 1.8em; padding: 15px; border-radius: 50%; margin-right: 20px; color: #fff; width: 30px; height: 30px; display: flex; align-items: center; justify-content: center; }
        .stat-card.total .icon { background-color: var(--primary-color); }
        .stat-card.passed .icon { background-color: var(--success-color); }
        .stat-card.failed .icon { background-color: var(--danger-color); }
        .stat-card.rows .icon { background-color: var(--info-color); }
        .stat-card.diffs .icon { background-color: var(--warning-color); }
        .stat-card.cols .icon { background-color: #7f8c8d; }
        .stat-card.tables .icon { background-color: #9b59b6; }
        .stat-card .info h3 { margin: 0 0 5px 0; font-size: 1em; color: var(--subtle-text-color); }
        .stat-card .info .count { font-size: 2em; font-weight: 700; color: var(--text-color); }
        .charts-container { display: grid; grid-template-columns: 1fr; gap: 30px; }
        .chart-card { background: var(--card-bg-color); padding: 30px; border-radius: 8px; border: 1px solid var(--border-color); }
        .chart-card h3 { margin-top: 0; margin-bottom: 15px; font-weight: 700; color: var(--text-color); }
        .chart-summary { font-size: 1.1em; margin-bottom: 20px; }
        .summary-matched { color: var(--success-color); font-weight: 700; }
        .summary-mismatched { color: var(--danger-color); font-weight: 700; }
        .chart-wrapper { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 40px; align-items: center; }
        .chart-inner { position: relative; height: 340px; }
        .overview-chart-card .chart-inner { position: relative; height: 260px; }
        .overview-chart-container .chart-inner, .analytics-chart { cursor: pointer; }
        table { width: 100%; border-collapse: collapse; font-size: 0.9em; }
        th, td { padding: 12px 15px; text-align: left; border-bottom: 1px solid var(--border-color); vertical-align: middle; word-break: break-word; }
        th { background-color: #e9ecef; font-weight: 700; color: #212529; }
        body.dark-mode th { background-color: #343a40; color: #f8f9fa; }
        tbody tr:hover { background-color: var(--header-bg-color); }
        .summary-table tbody tr { cursor: pointer; }
        .summary-table tbody tr.status-row-pass { background-color: rgba(40, 167, 69, 0.08); }
        .summary-table tbody tr.status-row-pass:hover { background-color: rgba(40, 167, 69, 0.15); }
        .summary-table tbody tr.status-row-fail { background-color: rgba(220, 53, 69, 0.08); }
        .summary-table tbody tr.status-row-fail:hover { background-color: rgba(220, 53, 69, 0.15); }
        .summary-table tbody tr.status-row-warn { background-color: rgba(255, 193, 7, 0.08); }
        .summary-table tbody tr.status-row-warn:hover { background-color: rgba(255, 193, 7, 0.15); }
        .summary-table .status-cell span { padding: 5px 12px; border-radius: 15px; font-size: 0.85em; color: #fff; font-weight: 600; }
        .summary-table .status-pass, .summary-table .status-passed { background-color: var(--success-color); }
        .summary-table .status-fail, .summary-table .status-failed, .summary-table .status-error, .summary-table .status-timeout { background-color: var(--danger-color); }
        .summary-table .status-skip { background-color: var(--warning-color); }
        .tab-content { display: none; }
        .tab-content.active { display: block; }
        .reports-controls { display: flex; justify-content: space-between; align-items: center; gap: 20px; flex-wrap: wrap; }
        .search-bar { width: 100%; max-width: 450px; position: relative; }
        .search-bar input { width: 100%; padding: 8px 15px 8px 35px; border-radius: 20px; border: 1px solid var(--border-color); background-color: var(--card-bg-color); color: var(--text-color); font-size: 0.9em; }
        .search-bar .fa-search { position: absolute; top: 50%; left: 15px; transform: translateY(-50%); color: var(--subtle-text-color); }
        .filter-controls { display: flex; gap: 10px; flex-wrap: wrap; }
        .filter-btn, .defect-filter-btn { padding: 8px 18px; border: 1px solid var(--border-color); background-color: var(--card-bg-color); color: var(--text-color); border-radius: 20px; cursor: pointer; font-size: 0.9em; font-weight: 600; transition: all 0.3s ease; }
        .filter-btn:hover, .defect-filter-btn:hover { border-color: var(--primary-color); color: var(--primary-color); }
        .filter-btn.active, .defect-filter-btn.active { background-color: var(--primary-color); color: #fff; border-color: var(--primary-color); }
        .collapsible .section-title { cursor: pointer; }
        .collapsible .toggle-icon { transition: transform 0.3s ease; }
        .collapsible.collapsed .toggle-icon { transform: rotate(-90deg); }
        .collapsible.collapsed .section-content { display: none; }
        .table-header { display: flex; justify-content: space-between; align-items: center; margin-top: 30px; border-bottom: 1px solid var(--border-color); padding-bottom: 10px; }
        .export-btn { padding: 4px 12px; font-size: 0.8em; background-color: var(--bg-color); border: 1px solid var(--border-color); color: var(--subtle-text-color); border-radius: 5px; cursor: pointer; }
        .export-btn:hover { background-color: var(--header-bg-color); color: var(--text-color); }
        .btn { padding: 5px 15px; font-size: 0.85em; border-radius: 5px; cursor: pointer; border: 1px solid var(--border-color); background-color: var(--card-bg-color); color: var(--text-color); font-weight: 600; transition: all 0.2s ease; }
        .btn:hover { border-color: var(--primary-color); color: var(--primary-color); }
        .scenario-summary-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 20px; margin-bottom: 30px; }
        .summary-block { background-color: var(--bg-color); padding: 15px; border-radius: 6px; border-left: 4px solid var(--info-color); }
        .summary-block h4 { margin: 0 0 8px 0; font-size: 0.9em; color: var(--subtle-text-color); font-weight: 600; }
        .summary-block p { margin: 0; font-size: 1.1em; font-weight: 700; word-break: break-all; }
        .summary-block.status-fail, .summary-block.status-error { border-left-color: var(--danger-color); }
        .summary-block.status-pass { border-left-color: var(--success-color); }
        .summary-block.status-fail p, .summary-block.status-error p { color: var(--danger-color); }
        .summary-block.status-pass p { color: var(--success-color); }
        .setting-item { display: flex; justify-content: space-between; align-items: center; padding: 15px 0; border-bottom: 1px solid var(--border-color); }
        .setting-item:last-child { border-bottom: none; }
        .setting-item h3 { margin: 0; font-size: 1em; }
        .theme-switch { position: relative; display: inline-block; width: 50px; height: 28px; }
        .theme-switch input { opacity: 0; width: 0; height: 0; }
        .slider { position: absolute; cursor: pointer; top: 0; left: 0; right: 0; bottom: 0; background-color: #ccc; transition: .4s; }
        .slider:before { position: absolute; content: ""; height: 20px; width: 20px; left: 4px; bottom: 4px; background-color: white; transition: .4s; }
        input:checked + .slider { background-color: var(--primary-color); }
        input:checked + .slider:before { transform: translateX(22px); }
        .slider.round { border-radius: 34px; }
        .slider.round:before { border-radius: 50%; }
        .font-size-controls { display: flex; align-items: center; gap: 15px; }
        #fontSizeSlider { width: 150px; cursor: pointer; }
        #fontSizeValue { font-weight: 700; min-width: 40px; text-align: center; }
        .query-details { margin-top: 20px; border: 1px solid var(--border-color); border-radius: 6px; }
        .query-details summary { cursor: pointer; padding: 10px 15px; background-color: var(--header-bg-color); font-weight: 600; }
        .query-box { padding: 15px; background-color: var(--bg-color); }
        .query-box h4 { margin-top: 0; margin-bottom: 5px; }
        .query-box pre { background-color: var(--card-bg-color); padding: 15px; border-radius: 4px; white-space: pre-wrap; word-wrap: break-word; font-size: 0.85em; }
        .detailed-report-section { border-left: 5px solid var(--border-color); }
        .detailed-report-section.status-pass { border-left-color: var(--success-color); }
        .detailed-report-section.status-pass .section-title { background-color: rgba(40, 167, 69, 0.1); }
        .detailed-report-section.status-fail { border-left-color: var(--danger-color); }
        .detailed-report-section.status-fail .section-title { background-color: rgba(220, 53, 69, 0.1); }
        .detailed-report-section.status-error, .detailed-report-section.status-timeout { border-left-color: var(--warning-color); }
        .detailed-report-section.status-error .section-title, .detailed-report-section.status-timeout .section-title { background-color: rgba(255, 193, 7, 0.1); }
        .sidebar-toggle-btn { position: fixed; top: 18px; left: 20px; z-index: 101; background: #002060; border: 2px solid var(--border-color); border-radius: 50%; width: 35px; height: 35px; font-size: 1.3em; cursor: pointer; display: flex; align-items: center; justify-content: center; color: #ffffff; transition: all 0.3s ease-in-out; }
        body:not(.sidebar-collapsed) .sidebar-toggle-btn { left: 20px; }
        .sidebar-toggle-btn:hover { color: var(--primary-color); box-shadow: 0 4px 10px rgba(0,0,0,0.1); }
        body.sidebar-collapsed .sidebar { transform: translateX(-100%); }
        body.sidebar-collapsed .main-content { margin-left: 0; padding-left: 70px; }
        body.sidebar-collapsed .sidebar-toggle-btn { left: 20px; }
        .execution-summary-header { display: flex; justify-content: space-between; align-items: center; }
        .execution-summary-header .actions { display: flex; gap: 10px; }
        #defects .summary-table tbody tr { background-color: rgba(220, 53, 69, 0.05); }
        #defects .summary-table tbody tr:hover { background-color: rgba(220, 53, 69, 0.1); }
        .kpi-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; }
        .kpi-card { background: var(--bg-color); border-radius: 8px; padding: 20px; position: relative; border: 1px solid var(--border-color); transition: all 0.3s ease-in-out; }
        .kpi-card h3 { font-size: 1.4em; color: var(--text-color); margin: 0 0 10px; font-weight: 700; }
        .kpi-card p { font-size: 1.2em; font-weight: 600; color: var(--subtle-text-color); margin: 0; word-break: break-all; }
        .kpi-card i { position: absolute; top: 20px; right: 20px; font-size: 1.8em; color: var(--primary-color); opacity: 0.2; }
        .defect-log-controls { display: flex; flex-wrap: wrap; justify-content: space-between; align-items: center; gap: 20px; margin-bottom: 25px; }
        #clear-defect-filter { display: none; }
        #chart-filter-status { display: none; margin-bottom: 20px; padding: 10px 15px; background-color: var(--header-bg-color); border-radius: 6px; border-left: 4px solid var(--primary-color); font-weight: 600; }
        #chart-filter-status span { margin-right: 15px; }
        #chart-filter-status button { font-size: 0.8em; padding: 3px 8px; }
        .summary-table thead th { cursor: pointer; position: relative; }
        .summary-table thead th::after { content: ''; display: inline-block; margin-left: 8px; opacity: 0.4; font-size: 0.8em; }
        .summary-table thead th[data-sort-dir="asc"]::after { content: '\\25B2'; opacity: 1; }
        .summary-table thead th[data-sort-dir="desc"]::after { content: '\\25BC'; opacity: 1; }
        body.pdf-export-mode .sidebar, body.pdf-export-mode .sidebar-toggle-btn, body.pdf-export-mode #reports .header, body.pdf-export-mode .execution-summary-header .actions, body.pdf-export-mode .reports-controls, body.pdf-export-mode .export-btn { display: none !important; }
        body.pdf-export-mode .main-content { margin-left: 0 !important; padding: 15px !important; }
        body.pdf-export-mode .section, body.pdf-export-mode .collapsible { box-shadow: none !important; border: 1px solid #ddd !important; page-break-inside: avoid !important; }
        @keyframes fadeIn { from { opacity: 0; transform: translateY(10px); } to { opacity: 1; transform: translateY(0); } }
        .main-content { animation: fadeIn 0.6s ease-in-out forwards; }
        .sidebar-nav li .tab-button, .btn, .filter-btn, .defect-filter-btn { transition: all 0.3s ease-in-out; }
        .stat-card:hover, .kpi-card:hover { transform: translateY(-8px) scale(1.02); box-shadow: 0 12px 25px rgba(0,0,0,0.1); }
        .collapsible:not(.collapsed) .section-content { display: block; max-height: 5000px; padding-top: 20px; padding-bottom: 15px; transition: max-height 0.7s cubic-bezier(0.25, 0.1, 0.25, 1.0), padding-top 0.7s ease, padding-bottom 0.7s ease; }
        .collapsible.collapsed .section-content { max-height: 0 !important; overflow: hidden !important; transition: max-height 0.5s cubic-bezier(0, 1, 0, 1), padding-top 0.5s ease, padding-bottom 0.5s ease; padding-top: 0 !important; padding-bottom: 0 !important; margin-top: 0 !important; margin-bottom: 0 !important; border: none !important; }
        .reveal { opacity: 0; transform: translateY(40px); transition: opacity 0.8s ease-out, transform 0.8s ease-out; }
        .reveal.visible { opacity: 1; transform: translateY(0); }
        #scenario-report .stat-card .info h3 { font-size: 1.5em; font-weight: 700; color: var(--text-color); }
        #scenario-report .stat-card .info .count { font-size: 1.3em; font-weight: 600; color: var(--subtle-text-color); }
    </style>
</head>
<body>

<div class="sidebar">
    <div class="sidebar-header"><h2>Data Quality Report</h2></div>
    <ul class="sidebar-nav">
        <li><button class="tab-button active" data-tab="dashboard"><i class="fas fa-tachometer-alt"></i> Dashboard</button></li>
        {% if enable_visual_report %}
        <li><button class="tab-button" data-tab="visuals"><i class="fas fa-chart-bar"></i> Visual Report</button></li>
        {% endif %}
        <li><button class="tab-button" data-tab="reports"><i class="fas fa-file-alt"></i> Data Quality Report</button></li>
        {% if profiling_enabled %}
        <li><button class="tab-button" data-tab="profiling"><i class="fas fa-search-plus"></i> Data Profiling</button></li>
        {% endif %}
        {% if enable_scenario_report %}
        <li><button class="tab-button" data-tab="scenario-report"><i class="fas fa-tasks"></i> DQ Validation Report</button></li>
        {% endif %}
        {% if enable_defects_tab %}
        <li><button class="tab-button" data-tab="defects"><i class="fas fa-bug"></i> Defects</button></li>
        {% endif %}
        <li><button class="tab-button" data-tab="settings"><i class="fas fa-cog"></i> Settings</button></li>
    </ul>

    {% if show_company_logo %}
    <div class="sidebar-footer">
                 <!-- type you logo here -->
                 <img src="data:image/png;base64,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" alt="Company Logo Placeholder">
    </div>
    {% endif %}
    <div class="sidebar-resize-handle"></div>

</div>

<main class="main-content">
    <button id="sidebar-toggle" class="sidebar-toggle-btn" title="Toggle Sidebar"><i class="fas fa-bars"></i></button>

    <div id="dashboard" class="tab-content active">
        <div class="page-layout">
            <header class="header" style="flex-direction: column; align-items: flex-start; margin-bottom: 30px;">
                <h1 style="width: 100%; text-align: center; margin-bottom: 10px; font-size: 2.5em;">Data Quality Dashboard</h1>
                <div style="width: 100%;">
                    <h2 style="font-size: 1.8em; font-weight: 800; color: var(--primary-color); margin: 0; line-height: 1.2;">Subject Area : {{ report_subjectarea }} </h2> 
                    <h2 style="font-size: 1.8em; font-weight: 800; color: var(--primary-color); margin: 5px 0 15px 0; line-height: 1.2;">Source: {{ report_domain }} </h2> 
                    <header class="header"><div class="timestamp">Version 1.0 </div></header>
            
                </div>
            </header>
            
            <div class="dashboard-main-grid">
                
                <div class="stats-grid">
                    <div class="stat-card cols"><div class="icon"><i class="fas fa-columns"></i></div><div class="info"><h3>Key Data Element</h3><span class="count"> {{ summary_stats.total_distinct_columns }} </span></div></div>
                    <div class="stat-card total"><div class="icon"><i class="fas fa-file-invoice"></i></div><div class="info"><h3>Total Scenarios</h3><span class="count">{{ summary_stats.total }}</span></div></div>
                    <div class="stat-card passed"><div class="icon"><i class="fas fa-check-circle"></i></div><div class="info"><h3>Passed</h3><span class="count">{{ summary_stats.success }}</span></div></div>
                    <div class="stat-card failed"><div class="icon"><i class="fas fa-times-circle"></i></div><div class="info"><h3>Failed</h3><span class="count">{{ summary_stats.failure }}</span></div></div>
                    
                    <div class="stat-card rows"><div class="icon"><i class="fas fa-database"></i></div><div class="info"><h3>All Tables Count </h3><span class="count">{{ summary_stats.total_tables }}</span></div></div>
                    <div class="stat-card failed"><div class="icon"><i class="fas fa-bug"></i></div><div class="info"><h3>Total Failure Count</h3><span class="count">{{ summary_stats.total_diffs }}</span></div></div>
                    <div class="stat-card diffs"><div class="icon"><i class="fas fa-percentage"></i></div><div class="info"><h3>Success Percentage</h3><span class="count">{{ summary_stats.success_percentage }}</span></div></div>
                    <div class="stat-card tables"><div class="icon"><i class="fas fa-table"></i></div><div class="info"><h3>Total Rows Checked</h3><span class="count">{{ summary_stats.total_rows_checked }}</span></div></div>
                    
             </div>
                
                <div class="overview-chart-card">
                    <h3 class="section-title">Test Scenario Status Overview</h3>
                    <div class="chart-inner">
                        <canvas id="dimensionMatrixPieChart">Chart could not be rendered.</canvas>
                    </div>
                </div>
            </div>
            
            <section class="section" id="dimension-matrix-section">
                <h2 class="section-title">Results by Dimension Matrix</h2>
                {% set dimension_data = dimension_matrix_chart_data_json | fromjson %}
                {% if dimension_data and dimension_data.labels and dimension_data.labels|length > 0 %}
                <div class="dashboard-overview" style="grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 30px;">
                    <div class="chart-card">
                        <h3 style="text-align: center;">Test Scenario Status per Dimension Matrix</h3>
                        <div class="chart-inner" style="height: 300px;">
                            <canvas id="dimensionMatrixBarChart">Chart could not be rendered.</canvas>
                        </div>
                    </div>
                    {% if enable_rows_by_dimension_pie %}
                    <div class="chart-card">
                        <h3 style="text-align: center;">Data Quality - Dimension Matrix By Row</h3>
                        <div class="chart-inner" style="height: 300px;">
                            <canvas id="rowsByDimensionPieChart">Chart could not be rendered.</canvas>
                        </div>
                    </div>
                    {% endif %}
                    
                    {% if enable_dimension_analysis %}
                    <div class="chart-card" style="grid-column: 1 / -1;">
                        <h3 style="text-align: center;">Dimension Analysis (Rows)</h3>
                        <div class="chart-inner" style="height: 350px;">
                            <canvas id="dimensionAnalysisChart">Chart could not be rendered.</canvas>
                        </div>
                    </div>
                    {% endif %}
                </div>
                {% else %}
                <p style="text-align:center; color: var(--subtle-text-color);">No data available to generate dimension matrix charts.</p>
                {% endif %}
            </section>
        </div>
    </div>
    
    {% if enable_visual_report %}
    <div id="visuals" class="tab-content">
        <div class="page-layout">
            <header class="header"><h1>Visual Report</h1></header>
            
            {% if defect_kpis and defect_kpis.total_defects != '0' %}
            <section class="section">
                <h2 class="section-title">Defect Summary</h2>
                <div class="kpi-grid">
                    <div class="kpi-card"><h3>Total Defects Found</h3><p>{{ defect_kpis.total_defects }}</p><i class="fas fa-bug"></i></div>
                    <div class="kpi-card"><h3>Most Problematic Table</h3><p>{{ defect_kpis.most_problematic_table }}</p><i class="fas fa-table"></i></div>
                    <div class="kpi-card"><h3>Top Failing Scenario</h3><p>{{ defect_kpis.top_failing_scenario }}</p><i class="fas fa-sitemap"></i></div>
                    <div class="kpi-card"><h3>% of Rules Failed</h3><p>{{ defect_kpis.percent_rules_failed }}</p><i class="fas fa-percentage"></i></div>
                </div>
            </section>
            {% endif %}

            {% set defect_analytics = defect_analytics_json | fromjson %}
            {% if defect_analytics and defect_kpis.total_defects != '0' %}
            <section class="section">
                <h2 class="section-title">Defect Analytics</h2>
                <div class="charts-container" style="grid-template-columns: 1fr 1fr; gap: 30px;">
                    {% if defect_analytics.hotspots and defect_analytics.hotspots.data %}<div class="chart-card"><h3 class="analytics-chart"> Defect Hotspots (Table.Column)</h3><div class="chart-inner" style="height: 350px;"><canvas class="analytics-chart" id="defectHotspotsChart"></canvas></div></div>{% endif %}
                    {% if defect_analytics.by_scenario and defect_analytics.by_scenario.data %}<div class="chart-card"><h3 class="analytics-chart">Defect By Data Quality Scenarios </h3><div class="chart-inner" style="height: 350px;"><canvas class="analytics-chart" id="defectsByScenarioChart"></canvas></div></div>{% endif %}
                    
                    {% if enable_defect_flow and defect_analytics.sankey_data and defect_analytics.sankey_data|length > 0 %}
                    <div class="chart-card" style="grid-column: 1 / -1;">
                        <h3>Defect Flow (Dimension → Table → Column)</h3>
                        <div class="chart-inner" style="height: 500px;"><canvas id="sankeyChart"></canvas></div>
                    </div>
                    {% endif %}
                    
                    {% if defect_analytics.treemap_data and defect_analytics.treemap_data|length > 0 %}
                    <div class="chart-card" style="grid-column: 1 / -1;">
                        <h3>Defect Concentration Heatmap</h3>
                        <div class="chart-inner" style="height: 450px;"><canvas id="treemapChart"></canvas></div>
                    </div>
                    {% endif %}

                    {% if defect_analytics.by_table and defect_analytics.by_table.data %}<div class="chart-card"><h3 class="analytics-chart">Defects by Table </h3><div class="chart-inner" style="height: 350px;"><canvas class="analytics-chart" id="defectsByTableChart"></canvas></div></div>{% endif %}
                    {% if defect_analytics.by_reason and defect_analytics.by_reason.data %}<div class="chart-card"><h3 class="analytics-chart">Defects by Reason</h3><div class="chart-inner" style="height: 350px;"><canvas class="analytics-chart" id="reasonBreakdownChart"></canvas></div></div>{% endif %}
                    
                </div>
            </section>
            {% else %}
             <section class="section"><p>No defect data available to generate visual analytics.</p></section>
            {% endif %}
        </div>
    </div>
    {% endif %}

    {% if enable_scenario_report %}
    <div id="scenario-report" class="tab-content">
        <div class="page-layout">
            <header class="header"><h1>DQ Validation Report</h1></header>
            {% set scenario_list = scenario_chart_data_json | fromjson %}
            {% for scenario in scenario_list %}
            <section class="section">
                <h2 class="section-title">
                    {% if scenario.scenario_name == 'NULL' %}
                        Validation Results for Null / Not Null
                    {% else %}
                        Validation Results for {{ scenario.scenario_name }}
                    {% endif %}
                </h2>
                <div class="dashboard-overview" style="grid-template-columns: 1fr;">
                    <div class="stats-grid">
                        <div class="stat-card rows">
                            <div class="icon"><i class="fas fa-exchange-alt"></i></div>
                            <div class="info">
                                <h3>Total Rows Compared</h3>
                                <span class="count">{{ scenario.total_compared }}</span>
                            </div>
                        </div>
                         <div class="stat-card">
                             <div class="info" style="flex-grow:1;">
                                <h3 style="text-align: center; font-size: 1.1em; margin-bottom: 15px;">Row Comparison</h3>
                                 <p class="chart-summary" style="margin-bottom: 0; text-align: center;">
                                     <span class="summary-matched">Matched Rows: {{ " {:,}".format(scenario.matched) }}</span><br>
                                     <span class="summary-mismatched">Mismatched Rows: {{ " {:,}".format(scenario.mismatched) }}</span>
                                </p>
                            </div>
                        </div>
                    </div>
                    
                    {% if (scenario.matched + scenario.mismatched) > 0 %}
                    <div class="chart-card">
                        <div class="chart-wrapper" style="gap: 30px; grid-template-columns: 1fr 2fr; align-items: center;">
                            <div class="chart-inner" style="height: 220px;">
                                <canvas id="scenarioPieChart_{{ loop.index0 }}"></canvas>
                            </div>
                            <div class="chart-inner" style="height: 220px;">
                                <canvas id="scenarioBarChart_{{ loop.index0 }}"></canvas>
                            </div>
                        </div>
                    </div>
                    {% else %}
                    <div class="chart-card" style="display: flex; align-items: center; justify-content: center; min-height: 282px;">
                        <p style="color: var(--subtle-text-color);">No row comparison data to display in charts.</p>
                    </div>
                    {% endif %}
                </div>
            </section>
            {% endfor %}
        </div>
    </div>
    {% endif %}

    <div id="reports" class="tab-content">
        <div class="page-layout">
            <header class="header"><h1>Detailed Reports</h1></header>
            <section class="section">
                 <div class="execution-summary-header">
                    <h2 class="section-title" style="margin: 0; padding: 0; border: none;">Execution Summary</h2>
                    <div class="actions">
                        <button id="export-pdf-btn" class="btn"><i class="fas fa-file-pdf"></i> Export to PDF</button>
                        <button id="expand-collapse-all" class="btn">Expand All</button>
                    </div>
                </div>
                <div style="overflow-x:auto; margin-top: 20px;">{{ summary_table_html | safe }}</div>
                <div class="reports-controls" style="margin-top: 20px; border-top: 1px solid var(--border-color); padding-top: 20px;">
                    <div class="search-bar"><i class="fas fa-search"></i><input type="text" id="reportSearch" placeholder="Search and filter detailed reports below..."></div>
                    <div class="filter-controls">
                        <button class="filter-btn active" data-filter="all">All</button>
                        <button class="filter-btn" data-filter="pass">Passed</button>
                        <button class="filter-btn" data-filter="fail">Failed</button>
                        <button class="filter-btn" data-filter="error">Errors</button>
                    </div>
                </div>
            </section>

            {% for report in detailed_reports %}
            <section class="section detailed-report-section collapsible collapsed status-{{ report.summary.Status | lower }}" data-status="{{ report.summary.Status | lower }}" data-tc-id="{{ report.summary['TC#'] }}" data-scenario-id="{{ report.summary.Report_Title }}">
                <h2 class="section-title"><span>{{ report.summary.Report_Title }}</span><i class="fas fa-chevron-down toggle-icon"></i></h2>
                <div class="section-content">
                    <div class="scenario-summary-grid" style="grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));">
                        <div class="summary-block"><h4>Target</h4><p>{{ report.summary.Full_Target_Name }}</p></div>
                        <div class="summary-block status-{{ report.summary.Status | lower }}"><h4>Status</h4><p>{{ report.summary.Status }}</p></div>
                        <div class="summary-block"><h4>Total Rows Checked</h4><p>{{ "{:,}".format(report.summary.Total_Count) }}</p></div>
                        <div class="summary-block status-{{ 'fail' if report.summary.Failure_Count > 0 else 'pass' }}"><h4>Failure Count</h4><p>{{ "{:,}".format(report.summary.Failure_Count) }}</p></div>
                        <div class="summary-block status-{{ 'fail' if report.summary.Failure_Count > 0 else 'pass' }}"><h4>Failure Percentage</h4><p>{{ report.summary.Failure_Percentage }}</p></div>
                        <div class="summary-block"><h4>Runtime</h4><p>{{ report.summary.Runtime }}s</p></div>
                        {% if report.summary.whereclause %}
                        <div class="summary-block"><h4>Where Clause Used</h4><p>{{ report.summary.whereclause }}</p></div>
                        {% endif %}
                    </div>
                    {% if report.summary.Details %}<div class="summary-block status-{{ report.summary.Status | lower }}" style="margin-top: 20px;"><h4>Details</h4><p>{{ report.summary.Details }}</p></div>{% endif %}
                    {% for title, table_html in report.dataframes.items() %}{% if table_html %}<div class="table-header"><h3>{{ title }}</h3><button class="export-btn">Export to CSV</button></div><div class="table-container" style="overflow-x:auto;">{{ table_html | safe }}</div>{% endif %}{% endfor %}
                </div>
            </section>
            {% endfor %}
        </div>
    </div>
    
    {% if profiling_enabled %}
    <div id="profiling" class="tab-content">
        <div class="page-layout">
            <header class="header"><h1>Data Profiling</h1></header>
            {% if profiling_results %}
                {% for profile in profiling_results %}
                <section class="section">
                    <h2 class="section-title">Table: {{ profile.table_name }}</h2>
                    <div class="scenario-summary-grid">
                        <div class="summary-block"><h4>Total Rows</h4><p>{{ "{:,}".format(profile.table_stats.row_count) }}</p></div>
                        <div class="summary-block"><h4>Total Columns</h4><p>{{ profile.table_stats.column_count }}</p></div>
                    </div>
                    <h3 style="margin-top: 30px; border-bottom: 1px solid var(--border-color); padding-bottom: 10px;">Column Analysis</h3>
                    <div style="overflow-x:auto;">
                        <table class="summary-table">
                            <thead>
                                <tr>
                                    <th>Column Name</th><th>Data Type</th><th>Nulls</th><th>Null %</th><th>Distinct Values</th><th>Distinct %</th>
                                    <th>Min</th><th>Max</th><th>Avg</th><th>Std Dev</th><th>Min Len</th><th>Max Len</th><th>Avg Len</th>
                                </tr>
                            </thead>
                            <tbody>
                                {% for col in profile.column_stats %}
                                <tr>
                                    <td>{{ col.column_name }}</td>
                                    <td>{{ col.data_type }}</td>
                                    <td>{{ "{:,}".format(col.null_count) }}</td>
                                    <td>{{ "%.2f"|format(col.null_percentage) }}%</td>
                                    <td>{{ "{:,}".format(col.distinct_count) }}</td>
                                    <td>{{ "%.2f"|format(col.distinct_percentage) }}%</td>
                                    <td>{{ col.get('min') if col.get('min') is not none else '-' }}</td>
                                    <td>{{ col.get('max') if col.get('max') is not none else '-' }}</td>
                                    <td>{{ "%.2f"|format(col.get('avg')) if col.get('avg') is not none else '-' }}</td>
                                    <td>{{ "%.2f"|format(col.get('std_dev')) if col.get('std_dev') is not none else '-' }}</td>
                                    <td>{{ col.get('min_len') if col.get('min_len') is not none else '-' }}</td>
                                    <td>{{ col.get('max_len') if col.get('max_len') is not none else '-' }}</td>
                                    <td>{{ "%.2f"|format(col.get('avg_len')) if col.get('avg_len') is not none else '-' }}</td>
                                </tr>
                                {% endfor %}
                            </tbody>
                        </table>
                    </div>
                </section>
                {% endfor %}
            {% else %}
                <section class="section">
                    <p>No data profiling results available. Please check the `Tables` sheet in your DQ_Scenarios.xlsx file.</p>
                </section>
            {% endif %}
        </div>
    </div>
    {% endif %}

    {% if enable_defects_tab %}
    <div id="defects" class="tab-content">
        <div class="page-layout">
            <header class="header"><h1>Defects</h1></header>
            {% if defect_table_html and defect_table_html.strip()|length > 200 %}
            <div id="chart-filter-status"></div>
            <div class="defect-log-controls">
                <div class="search-bar"><i class="fas fa-search"></i><input type="text" id="defectSearch" placeholder="Search defects..."></div>
                <div class="filter-controls">
                    <button class="defect-filter-btn active" data-filter="all">All</button>
                    <button class="defect-filter-btn" data-filter="Value Mismatch">Value Mismatch</button>
                    <button class="defect-filter-btn" data-filter="Execution Error">Execution Error</button>
                    <button class="defect-filter-btn" data-filter="Timeout">Timeout</button>
                    <button class="btn" id="clear-defect-filter"><i class="fas fa-times"></i> Clear Chart Filter</button>
                </div>
            </div>
            <section class="section">
                <div class="table-header" style="margin-top:0">
                    <h2 class="section-title" style="margin: 0; padding: 0; border: none;">Defect Log</h2>
                    <button class="export-btn" id="export-defects-btn">Export to CSV</button>
                </div>
                <div class="table-container" style="overflow-x:auto; margin-top:15px;">{{ defect_table_html | safe }}</div>
            </section>
            {% else %}
            <section class="section"><h2 class="section-title">Defect Log</h2><p>No defects were found during the comparison.</p></section>
            {% endif %}
        </div>
    </div>
    {% endif %}

    <div id="settings" class="tab-content">
        <div class="page-layout">
            <header class="header"><h1>Settings</h1></header>
            <section class="section">
                <h2 class="section-title">Appearance</h2>
                <div class="setting-item"><h3>Theme</h3><label class="theme-switch" for="theme-toggle"><input type="checkbox" id="theme-toggle" /><span class="slider round"></span></label></div>
                <div class="setting-item"><h3>Text Size</h3><div class="font-size-controls"><input type="range" id="fontSizeSlider" min="80" max="120" value="100"><span id="fontSizeValue">100%</span></div></div>
            </section>
        </div>
    </div>
</main>
<script>
document.addEventListener('DOMContentLoaded', function() {
    let activeCharts = [];
    let dashboardChartsInitialized = false;
    let defectChartsInitialized = false;
    let scenarioChartsInitialized = false;
    
    const applyReportFilters = () => {
        const searchTerm = document.getElementById('reportSearch').value.toLowerCase();
        const activeStatusFilter = document.querySelector('#reports .filter-btn.active').dataset.filter;
        const reportSections = document.querySelectorAll('.detailed-report-section');

        reportSections.forEach(section => {
            const status = section.dataset.status ? section.dataset.status.toLowerCase() : '';
            const content = section.textContent.toLowerCase();
            const statusMatch = (activeStatusFilter === 'all') || (status === activeStatusFilter);
            const searchMatch = content.includes(searchTerm);

            if (statusMatch && searchMatch) {
                section.style.display = '';
            } else {
                section.style.display = 'none';
            }
        });
    };
    
    const applyDefectFilters = () => {
        const searchTerm = document.getElementById('defectSearch').value.toLowerCase();
        const activeStatusFilter = document.querySelector('#defects .defect-filter-btn.active').dataset.filter;
        const defectTable = document.querySelector('#defects .details-table.summary-table');
        if (!defectTable) return;

        const headers = Array.from(defectTable.querySelectorAll('thead th'));
        const reasonColIndex = headers.findIndex(th => th.textContent.trim().toLowerCase() === 'reason');

        if (reasonColIndex === -1) {
            console.warn("Could not find 'Reason' column for filtering.");
            return;
        }

        defectTable.querySelectorAll('tbody tr').forEach(row => {
            const content = row.textContent.toLowerCase();
            const reasonText = row.cells[reasonColIndex]?.textContent.trim() || '';

            const statusMatch = (activeStatusFilter === 'all') || (reasonText === activeStatusFilter);
            const searchMatch = content.includes(searchTerm);

            row.style.display = (statusMatch && searchMatch) ? '' : 'none';
        });
    };

    const exportTableToCSV = (table, filename) => {
        if (!table) { console.error("Export failed: table element not found."); return; }
        let csv = [];
        table.querySelectorAll("tr").forEach(row => {
            let rowData = [];
            row.querySelectorAll("td, th").forEach(col => {
                let data = col.innerText.replace(/"/g, '""');
                rowData.push(`"${data}"`);
            });
            csv.push(rowData.join("\\n"));
        });
        const csvFile = new Blob([csv.join("\\n")], { type: "text/csv;charset=utf-8;" });
        const downloadLink = document.createElement("a");
        downloadLink.href = window.URL.createObjectURL(csvFile);
        downloadLink.download = filename;
        document.body.appendChild(downloadLink);
        downloadLink.click();
        document.body.removeChild(downloadLink);
    };

    const makeTableSortable = (table) => {
        const headers = table.querySelectorAll('thead th');
        headers.forEach((th, colIndex) => {
            th.addEventListener('click', () => {
                const tbody = table.querySelector('tbody');
                if (!tbody) return;
                const rows = Array.from(tbody.rows);
                const currentSortDir = th.dataset.sortDir;
                const newSortDir = currentSortDir === 'asc' ? 'desc' : 'asc';
                headers.forEach(h => delete h.dataset.sortDir);
                th.dataset.sortDir = newSortDir;
                const sortedRows = rows.sort((a, b) => {
                    let valA = a.cells[colIndex].textContent.trim();
                    let valB = b.cells[colIndex].textContent.trim();
                    const numA = parseFloat(valA.replace(/[^0-9.,-]/g, ''));
                    const numB = parseFloat(valB.replace(/[^0-9.,-]/g, ''));
                    let comparison = 0;
                    if (!isNaN(numA) && !isNaN(numB)) {
                        comparison = numA > numB ? 1 : -1;
                    } else {
                        comparison = valA.localeCompare(valB, undefined, {numeric: true});
                    }
                    return newSortDir === 'asc' ? comparison : -comparison;
                });
                sortedRows.forEach(row => tbody.appendChild(row));
            });
        });
    };

    const setTheme = (isDark) => { 
        document.body.classList.toggle('dark-mode', isDark); 
        localStorage.setItem('theme', isDark ? 'dark' : 'light'); 
        
        if (dashboardChartsInitialized) {
            activeCharts.forEach(chart => chart.destroy());
            activeCharts = [];
            initDashboardCharts(); 
            if (defectChartsInitialized) { initDefectCharts(); } 
            if (scenarioChartsInitialized) { initScenarioCharts(); }
        }
    };
    
    const setFontSize = (size) => { document.documentElement.style.fontSize = `${size}%`; const valueEl = document.getElementById('fontSizeValue'); if (valueEl) valueEl.textContent = `${size}%`; localStorage.setItem('fontSize', size); };
    
    const setSidebarState = (collapsed) => { 
        document.body.classList.toggle('sidebar-collapsed', collapsed); 
        localStorage.setItem('sidebarCollapsed', collapsed); 
        const mainContent = document.querySelector('.main-content');
        const sidebar = document.querySelector('.sidebar');
        
        if (collapsed) {
             mainContent.style.marginLeft = '0';
        } else {
             const storedWidth = localStorage.getItem('sidebarWidth') || '250';
             mainContent.style.marginLeft = storedWidth + 'px';
             sidebar.style.width = storedWidth + 'px';
        }
    };

    window.openTab = (tabName) => {
        if (!tabName) return;
        document.querySelectorAll('.tab-content.active').forEach(c => c.classList.remove('active'));
        document.querySelectorAll('.tab-button.active').forEach(b => b.classList.remove('active'));

        const tabContent = document.getElementById(tabName);
        const tabButton = document.querySelector(`.tab-button[data-tab="${tabName}"]`);

        if (tabContent) tabContent.classList.add('active');
        else console.error(`Tab content with id "${tabName}" not found.`);
        
        if (tabButton) tabButton.classList.add('active');

        if (typeof Chart !== 'undefined') {
            if (tabName === 'dashboard' && !dashboardChartsInitialized) { initDashboardCharts(); dashboardChartsInitialized = true; }
            if (tabName === 'visuals' && !defectChartsInitialized) { initDefectCharts(); defectChartsInitialized = true; }
            if (tabName === 'scenario-report' && !scenarioChartsInitialized) { initScenarioCharts(); scenarioChartsInitialized = true; }
        }
        if (tabName === 'reports') { styleSummaryTable(); }
    };

    const initDashboardCharts = () => {
        const isDark = document.body.classList.contains('dark-mode');
        const textColor = isDark ? '#e0e0e0' : '#343a40';
        initDimensionMatrixCharts(isDark, textColor);
    };
    
    const initDimensionMatrixCharts = (isDark, textColor) => {
        const dimensionData = {{ dimension_matrix_chart_data_json | safe }};
        if (!dimensionData || !dimensionData.labels || dimensionData.labels.length === 0) return;
        
        try {
            const pieCtx = document.getElementById('dimensionMatrixPieChart')?.getContext('2d');
            if (pieCtx) { activeCharts.push(new Chart(pieCtx, { type: 'pie', data: { labels: ['Passed', 'Failed'], datasets: [{ data: [dimensionData.pass_counts.reduce((a, b) => a + b, 0), dimensionData.fail_counts.reduce((a, b) => a + b, 0)], backgroundColor: ['#28a745', '#dc3545'], borderWidth: 4, borderColor: isDark ? '#1e1e1e' : '#fff' }] }, options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { position: 'bottom', labels: { color: textColor, font: { size: 14 } } }, datalabels: { color: '#fff', font: { weight: 'bold', size: 16 } } } } })); }
        } catch(e) { console.error("Error initializing Dimension Pie Chart:", e); }
        
        try {
            const barCtx = document.getElementById('dimensionMatrixBarChart')?.getContext('2d');
            if (barCtx) { activeCharts.push(new Chart(barCtx, { type: 'bar', data: { labels: dimensionData.labels, datasets: [ 
                { label: 'Passed', data: dimensionData.pass_counts, backgroundColor: '#28a745', borderColor: isDark ? '#333' : '#fff', borderWidth: 1 }, 
                { label: 'Failed', data: dimensionData.fail_counts, backgroundColor: '#dc3545', borderColor: isDark ? '#333' : '#fff', borderWidth: 1 } 
            ] }, options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { position: 'top', labels: { color: textColor } }, datalabels: { color: '#fff', font: { weight: 'bold' }, formatter: (value) => value > 0 ? value : null } }, scales: { x: { stacked: true, ticks: { color: textColor } }, y: { stacked: true, ticks: { color: textColor, beginAtZero: true } } } } })); }
        } catch(e) { console.error("Error initializing Dimension Bar Chart:", e); }

        {% if enable_rows_by_dimension_pie %}
        try {
            const rowsPieCtx = document.getElementById('rowsByDimensionPieChart')?.getContext('2d');
            if (rowsPieCtx) {
                const blueColor = '#36a2eb'; 
                const dimensionLabels = dimensionData.labels;
                const otherColors = ['#ff6384', '#4bc0c0', '#ff9f40', '#9966ff', '#ffcd56', '#e7e9ed', '#8e44ad', '#2ecc71'];

                const colors = dimensionLabels.map((label, i) => {
                    if (label.toLowerCase() === 'completeness') {
                        return blueColor; 
                    }
                    return otherColors[i % otherColors.length];
                });

                activeCharts.push(new Chart(rowsPieCtx, {
                    type: 'doughnut', 
                    data: { 
                        labels: dimensionData.labels, 
                        datasets: [{ 
                            data: dimensionData.total_records, 
                            backgroundColor: colors, 
                            borderWidth: 4, 
                            borderColor: isDark ? '#1e1e1e' : '#fff' 
                        }] 
                    }, 
                    options: { 
                        responsive: true, 
                        maintainAspectRatio: false, 
                        plugins: { 
                            legend: { position: 'right', labels: { color: textColor } }, 
                            datalabels: { 
                                color: '#fff', 
                                font: { weight: 'bold', size: 16 },
                                formatter: (value, context) => {
                                    const total = context.chart.data.datasets[0].data.reduce((a, b) => a + b, 0);
                                    const percentage = (value * 100 / total) || 0;
                                    return percentage > 0 ? percentage.toFixed(1) + '%' : null; 
                                }
                            } 
                        },
                        tooltips: {
                            callbacks: {
                                label: function(tooltipItem, data) {
                                    const label = data.labels[tooltipItem.index] || '';
                                    const value = data.datasets[0].data[tooltipItem.index];
                                    const total = data.datasets[0].data.reduce((a, b) => a + b, 0);
                                    const percentage = (value * 100 / total).toFixed(1);
                                    return `${label}: ${value.toLocaleString()} rows (${percentage}%)`;
                                }
                            }
                        }
                    } 
                })); 
            }
        } catch(e) { console.error("Error initializing Rows by Dimension Pie Chart:", e); }
        {% endif %}

        {% if enable_dimension_analysis %}
        try {
            const analysisCtx = document.getElementById('dimensionAnalysisChart')?.getContext('2d');
            if (analysisCtx && dimensionData.total_failures) {
                const commonBarBorder = { borderColor: 'rgba(52, 58, 64, 0.8)', borderWidth: 1, borderRadius: 4 };
                activeCharts.push(new Chart(analysisCtx, {
                    type: 'bar',
                    data: { labels: dimensionData.labels, datasets: [
                        { label: 'Total Rows', data: dimensionData.total_records, backgroundColor: 'rgba(40, 167, 69, 0.6)', ...commonBarBorder },
                        { label: 'Total Failing Rows', data: dimensionData.total_failures, backgroundColor: 'rgba(220, 53, 69, 0.7)', ...commonBarBorder }
                    ]},
                    options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { position: 'top', labels: { color: textColor } }, tooltip: { mode: 'index', intersect: false } },
                        scales: { 
                            x: { ticks: { color: textColor } }, 
                            y: { type: 'linear', display: true, position: 'left', title: { display: true, text: 'Count (Rows)', color: textColor }, ticks: { color: textColor, callback: function(value) {
                                if (value >= 1000000000) { return (value / 1000000000).toFixed(1).replace(/\\.0$/, '') + ' billion'; }
                                if (value >= 1000000) { return (value / 1000000).toFixed(1).replace(/\\.0$/, '') + ' million'; }
                                if (value >= 1000) { return (value / 1000).toFixed(0) + '(k)'; }
                                if (Number.isInteger(value)) { return value; }
                                return null;
                            } } }
                        }
                    }
                }));
            }
        } catch(e) { console.error("Error initializing Dimension Analysis Chart:", e); }
        {% endif %}
    };

    const initScenarioCharts = () => {
        const scenarioData = {{ scenario_chart_data_json | safe }};
        const isDark = document.body.classList.contains('dark-mode');
        const textColor = isDark ? '#e0e0e0' : '#343a40';
        scenarioData.forEach((scenario, index) => {
            if ((scenario.matched + scenario.mismatched) > 0) {
                try {
                    const pieCtx = document.getElementById(`scenarioPieChart_${index}`)?.getContext('2d');
                    if (pieCtx) { activeCharts.push(new Chart(pieCtx, { type: 'pie', data: { labels: ['Matched', 'Mismatched'], datasets: [{ data: [scenario.matched, scenario.mismatched], backgroundColor: ['#28a745', '#dc3545'], borderColor: isDark ? '#1e1e1e' : '#fff' }] }, options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { position: 'right', labels: { color: textColor } }, datalabels: { display: true, formatter: (v) => v > 0 ? v : '', color: '#fff', font: { weight: 'bold', size: 14 } } } } })); }
                    const barCtx = document.getElementById(`scenarioBarChart_${index}`)?.getContext('2d');
                    if (barCtx) { activeCharts.push(new Chart(barCtx, { type: 'bar', data: { labels: ['Mismatched', 'Matched'], datasets: [{ data: [scenario.mismatched, scenario.matched], backgroundColor: ['#dc3545', '#28a745'] }] }, options: { indexAxis: 'y', responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false }, datalabels: { display: true, anchor: 'end', align: 'end', color: textColor, font: { weight: 'bold' } } }, scales: { x: { ticks: { color: textColor } }, y: { ticks: { color: textColor } } } } })); }
                } catch(e) { console.error(`Could not render charts for scenario ${scenario.scenario_name}:`, e); }
            }
        });
    };

    const filterDefectTable = (columnName, value) => {
        if (columnName.toLowerCase() !== 'reason') {
            openTab('defects');
            return;
        }
        openTab('defects');
        const filterButtons = document.querySelectorAll('#defects .defect-filter-btn');
        let buttonFound = false;
        filterButtons.forEach(btn => {
            if (btn.dataset.filter === value) {
                document.querySelector('#defects .defect-filter-btn.active').classList.remove('active');
                btn.classList.add('active');
                buttonFound = true;
            }
        });

        if (!buttonFound) {
             document.querySelector('#defects .defect-filter-btn.active').classList.remove('active');
             document.querySelector('#defects .defect-filter-btn[data-filter="all"]').classList.add('active');
        }
        applyDefectFilters();
        const filterStatus = document.getElementById('chart-filter-status');
        const clearBtn = document.getElementById('clear-defect-filter');
        if (filterStatus && clearBtn) {
            filterStatus.style.display = 'flex';
            filterStatus.innerHTML = `<span><i class="fas fa-filter"></i> Filtered by <b>${columnName}:</b> ${value}</span>`;
            clearBtn.style.display = 'inline-block';
        }
    };

    const clearDefectFilter = () => {
        document.querySelector('#defects .defect-filter-btn.active').classList.remove('active');
        document.querySelector('#defects .defect-filter-btn[data-filter="all"]').classList.add('active');
        document.getElementById('defectSearch').value = '';
        applyDefectFilters();
        document.getElementById('chart-filter-status').style.display = 'none';
        document.getElementById('clear-defect-filter').style.display = 'none';
    };

    const initDefectCharts = () => {
        try {
            const isDark = document.body.classList.contains('dark-mode');
            const textColor = isDark ? '#e0e0e0' : '#343a40';
            const generateChartColors = (num) => ['rgba(54, 162, 235, 0.8)','rgba(255, 99, 132, 0.8)','rgba(75, 192, 192, 0.8)','rgba(255, 206, 86, 0.8)','rgba(153, 102, 255, 0.8)','rgba(255, 159, 64, 0.8)'].slice(0, num);
            const defectAnalytics = {{ defect_analytics_json | safe }};
            
            const chartClickHandler = (evt, elements, chart, columnName) => { if (elements.length > 0) { const label = chart.data.labels[elements[0].index]; filterDefectTable(columnName, label); } };
            const barOptions = (axis = 'y') => ({ indexAxis: axis, responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false }, datalabels: { color: '#fff', font: { weight: 'bold' } } }, scales: { x: { ticks: { color: textColor } }, y: { ticks: { color: textColor } } } });
            
            const chartConfigs = [
                { id: 'defectHotspotsChart', data: defectAnalytics.hotspots, type: 'bar', options: barOptions(), filterCol: 'Failing Column' },
                { id: 'defectsByScenarioChart', data: defectAnalytics.by_scenario, type: 'bar', options: barOptions(), filterCol: 'Scenario' },
            ];

            chartConfigs.forEach(config => {
                if (config.data && config.data.data) {
                    try {
                        const ctx = document.getElementById(config.id)?.getContext('2d');
                        if (ctx) {
                            const chartConfig = { type: config.type, data: { labels: config.data.labels, datasets: [{ data: config.data.data, backgroundColor: generateChartColors(config.data.labels.length) }] }, options: config.options };
                            chartConfig.options.onClick = (e, el, c) => chartClickHandler(e, el, c, config.filterCol);
                            activeCharts.push(new Chart(ctx, chartConfig));
                        }
                    } catch (e) { console.error(`Failed to init chart: ${config.id}`, e); }
                }
            });
            
            if (defectAnalytics.sankey_data && defectAnalytics.sankey_data.length > 0) {
                try {
                    const ctx = document.getElementById('sankeyChart')?.getContext('2d');
                    if (ctx) {
                        const sankeyPalette = ['#36a2eb', '#ff6384', '#4bc0c0', '#ff9f40', '#9966ff', '#ffcd56', '#c9cbcf', '#e7e9ed', '#8e44ad', '#2ecc71'];
                        const nodeColors = {};
                        let colorIndex = 0;
                        const nodes = {};

                        defectAnalytics.sankey_data.forEach(item => {
                            if (!nodeColors[item.from]) { nodeColors[item.from] = sankeyPalette[colorIndex++ % sankeyPalette.length]; }
                            if (!nodeColors[item.to]) { nodeColors[item.to] = sankeyPalette[colorIndex++ % sankeyPalette.length]; }
                            if (!nodes[item.from]) { nodes[item.from] = { total: 0 }; }
                            if (!nodes[item.to]) { nodes[item.to] = { total: 0 }; }
                            nodes[item.from].total += item.flow;
                            nodes[item.to].total += item.flow;
                        });
                        
                        activeCharts.push(new Chart(ctx, {
                            type: 'sankey',
                            data: {
                                datasets: [{
                                    data: defectAnalytics.sankey_data,
                                    colorFrom: (c) => nodeColors[c.raw.from],
                                    colorTo: (c) => nodeColors[c.raw.to],
                                    colorMode: 'gradient',
                                    labels: { display: false }
                                }]
                            },
                            options: {
                                responsive: true,
                                maintainAspectRatio: false,
                                layout: { padding: { top: 20, bottom: 20 } },
                                plugins: {
                                    datalabels: { display: false },
                                    legend: { display: false },
                                    tooltip: {
                                        enabled: true,
                                        callbacks: {
                                            title: (context) => {
                                                const item = context[0];
                                                if (item.element?.constructor.name === 'SankeyNode') { return item.element.label; }
                                                return '';
                                            },
                                            label: (context) => {
                                                const item = context;
                                                const raw = item.raw;
                                                if (item.element?.constructor.name === 'SankeyNode') {
                                                    const nodeLabel = item.element.label;
                                                    const total = nodes[nodeLabel]?.total || 0;
                                                    return `Total Flow: ${total.toLocaleString()}`;
                                                }
                                                if (raw && raw.from && raw.to) {
                                                   return `${raw.from} → ${raw.to}: ${raw.flow.toLocaleString()}`;
                                                }
                                                return '';
                                            }
                                        }
                                    }
                                }
                            }
                        }));
                    }
                } catch (e) {
                    console.error("Failed to init Sankey chart", e);
                }
            }
            
            if (defectAnalytics.treemap_data && defectAnalytics.treemap_data.length > 0) {
                 try {
                    const ctx = document.getElementById('treemapChart')?.getContext('2d');
                    if (ctx) {
                        const palette = ['#36a2eb', '#ff6384', '#4bc0c0', '#ff9f40', '#9966ff', '#ffcd56', '#8e44ad'];
                        const blockColors = {}; let colorIndex = 0;
                        defectAnalytics.treemap_data.forEach(item => { const key = `${item.Target_Table}_${item.Column_Name}`; if (!blockColors[key]) { blockColors[key] = palette[colorIndex++ % palette.length]; } });
                        activeCharts.push(new Chart(ctx, { type: 'treemap', data: { datasets: [{ tree: defectAnalytics.treemap_data, key: 'Failure_Count', groups: ['Target_Table', 'Column_Name'], backgroundColor: (c) => blockColors[`${c.raw?._data.Target_Table}_${c.raw?._data.Column_Name}`] || '#ccc', borderColor: 'rgba(255,255,255,0.7)', borderWidth: 1.5, labels: { display: true, color: '#fff', font: { size: 13, weight: 'bold' }, formatter: (c) => c.raw.treeNode?.children ? c.raw.g.toUpperCase() : `${c.raw.g} (${c.raw.v})` } }] }, options: { responsive: true, maintainAspectRatio: false, plugins: { datalabels: { display: false }, legend: { display: false }, tooltip: { callbacks: { title: () => '', label: (c) => ` ${c.raw.g}: ${c.raw.v}` } } } } }));
                        
                    }
                } catch(e) { console.error("Failed to init Treemap chart", e); }
            }
            
            const chartConfigs2 = [
                { id: 'defectsByTableChart', data: defectAnalytics.by_table, type: 'bar', options: barOptions('x'), filterCol: 'Table' },
                { id: 'reasonBreakdownChart', data: defectAnalytics.by_reason, type: 'doughnut', options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { position: 'right', labels: { color: textColor } }, datalabels: { color: '#fff', font: { weight: 'bold' } } }, onClick: (e, el, c) => chartClickHandler(e, el, c, 'Reason') }, filterCol: 'Reason' }
            ];

            chartConfigs2.forEach(config => {
                if (config.data && config.data.data) {
                    try {
                        const ctx = document.getElementById(config.id)?.getContext('2d');
                        if (ctx) {
                            const chartConfig = { type: config.type, data: { labels: config.data.labels, datasets: [{ data: config.data.data, backgroundColor: generateChartColors(config.data.labels.length) }] }, options: config.options };
                            chartConfig.options.onClick = (e, el, c) => chartClickHandler(e, el, c, config.filterCol);
                            activeCharts.push(new Chart(ctx, chartConfig));
                        }
                    } catch (e) { console.error(`Failed to init chart: ${config.id}`, e); }
                }
            });


        } catch(e) { console.error("Error initializing defect charts:", e); }
    };
    
    const styleSummaryTable = () => {
        const summaryTable = document.querySelector('#reports .summary-table');
        if (!summaryTable || summaryTable.dataset.styled) return;

        makeTableSortable(summaryTable);

        const headers = Array.from(summaryTable.querySelectorAll('thead th'));
        const tcColIndex = headers.findIndex(th => th.textContent.trim() === 'TC#');
        const statusColIndex = headers.findIndex(th => th.textContent.trim() === 'Overall_Status');
        
        summaryTable.querySelectorAll('tbody tr').forEach(row => {
            if (statusColIndex > -1) {
                const statusCell = row.cells[statusColIndex];
                if(statusCell) {
                    const status = statusCell.textContent.trim().toLowerCase();
                    row.className = `status-row-${status.replace('passed', 'pass').replace('failed', 'fail')}`;
                    if (statusCell.childElementCount === 0) {
                        statusCell.innerHTML = `<span class="status-cell-inner status-${status}">${statusCell.textContent}</span>`;
                        statusCell.classList.add('status-cell');
                    }
                }
            }
            
            if (tcColIndex > -1) {
                const tcId = row.cells[tcColIndex]?.textContent.trim();
                if (tcId) {
                    row.dataset.tcId = tcId;
                }
            }
        });
        summaryTable.dataset.styled = "true";
    };
    
    
    const sidebar = document.querySelector('.sidebar');
    const mainContent = document.querySelector('.main-content');
    const resizeHandle = document.querySelector('.sidebar-resize-handle');
    const minWidth = 150;
    const maxWidth = 500;

    if (resizeHandle && sidebar && mainContent) {
        let isResizing = false;

        resizeHandle.addEventListener('mousedown', function(e) {
            isResizing = true;
            document.body.style.userSelect = 'none';
            document.body.style.cursor = 'col-resize';
            sidebar.style.transition = 'none';
            mainContent.style.transition = 'none';
        });

        document.addEventListener('mousemove', function(e) {
            if (!isResizing) return;
            let newWidth = e.clientX;
            
            if (newWidth < minWidth) newWidth = minWidth;
            if (newWidth > maxWidth) newWidth = maxWidth;

            sidebar.style.width = newWidth + 'px';
            mainContent.style.marginLeft = newWidth + 'px';
            localStorage.setItem('sidebarWidth', newWidth);
        });

        document.addEventListener('mouseup', function() {
            if (isResizing) {
                isResizing = false;
                document.body.style.userSelect = '';
                document.body.style.cursor = '';
                sidebar.style.transition = 'transform 0.3s ease-in-out, width 0.3s ease-in-out';
                mainContent.style.transition = 'margin-left 0.3s ease-in-out, padding-left 0.3s ease-in-out';
            }
        });
        
        const storedWidth = localStorage.getItem('sidebarWidth');
        if (storedWidth) {
            sidebar.style.width = storedWidth + 'px';
            if (!document.body.classList.contains('sidebar-collapsed')) {
                 mainContent.style.marginLeft = storedWidth + 'px';
            }
        }
        
        document.getElementById('sidebar-toggle')?.addEventListener('click', () => {
             const isCollapsed = document.body.classList.contains('sidebar-collapsed');
             const currentWidth = sidebar.style.width || '250px';
             
             if (isCollapsed) {
                 sidebar.style.width = storedWidth ? storedWidth + 'px' : '250px';
                 mainContent.style.marginLeft = storedWidth ? storedWidth + 'px' : '250px';
             } else {
                 localStorage.setItem('sidebarWidth', currentWidth.replace('px', ''));
                 mainContent.style.marginLeft = '0';
             }
             setSidebarState(!isCollapsed);
        });
        
        if (document.body.classList.contains('sidebar-collapsed')) {
             mainContent.style.marginLeft = '0';
        }
    }


    try {
        if (!window.Chart || !window.ChartDataLabels) {
            throw new Error('Chart.js or its plugins failed to load. Charts will be unavailable.');
        }
        
        Chart.register(ChartDataLabels);
        
        const themeToggle = document.getElementById('theme-toggle');
        if (themeToggle) {
            const isDark = localStorage.getItem('theme') === 'dark';
            themeToggle.checked = isDark;
            setTheme(isDark);
        } else {
             setTheme(localStorage.getItem('theme') === 'dark');
        }
        
        setFontSize(localStorage.getItem('fontSize') || '100');
        
        const isCollapsed = localStorage.getItem('sidebarCollapsed') === 'true';
        setSidebarState(isCollapsed);
        if (!isCollapsed && sidebar && mainContent) {
            const initialWidth = localStorage.getItem('sidebarWidth') || '250';
            sidebar.style.width = initialWidth + 'px';
            mainContent.style.marginLeft = initialWidth + 'px';
        }


        initDashboardCharts();
        dashboardChartsInitialized = true;
        
        document.getElementById('theme-toggle')?.addEventListener('change', (e) => setTheme(e.target.checked));
        document.getElementById('fontSizeSlider')?.addEventListener('input', (e) => setFontSize(e.target.value));
        
        document.querySelectorAll('.sidebar .tab-button').forEach(button => {
            button.addEventListener('click', (e) => {
                openTab(e.currentTarget.dataset.tab);
            });
        });

        document.getElementById('reportSearch')?.addEventListener('keyup', applyReportFilters);

        document.querySelectorAll('#reports .filter-btn').forEach(button => {
            button.addEventListener('click', (e) => {
                document.querySelector('#reports .filter-btn.active').classList.remove('active');
                e.currentTarget.classList.add('active');
                applyReportFilters();
            });
        });
        
        document.querySelectorAll('#defects .defect-filter-btn').forEach(button => {
            button.addEventListener('click', (e) => {
                document.querySelector('#defects .defect-filter-btn.active').classList.remove('active');
                e.currentTarget.classList.add('active');
                applyDefectFilters();
            });
        });

        document.getElementById('defectSearch')?.addEventListener('keyup', applyDefectFilters);
        document.getElementById('clear-defect-filter')?.addEventListener('click', clearDefectFilter);

        const pdfExportBtn = document.getElementById('export-pdf-btn');
        if (pdfExportBtn) {
            pdfExportBtn.addEventListener('click', () => {
                const originalBtnText = pdfExportBtn.innerHTML;
                pdfExportBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Generating...';
                pdfExportBtn.disabled = true;

                const element = document.querySelector('.main-content');
                const reportTitle = document.querySelector('.sidebar-header h2').textContent.trim().replace(/\\s+/g, '_');
                const timestamp = new Date().toISOString().slice(0, 10);
                
                const opt = {
                    margin: [5, 5, 10, 5],
                    filename: `${reportTitle}_${timestamp}.pdf`,
                    image: { type: 'jpeg', quality: 0.98 },
                    html2canvas: { scale: 2, useCORS: true, logging: false },
                    jsPDF: { unit: 'mm', format: 'a3', orientation: 'landscape' }
                };

                document.body.classList.add('pdf-export-mode');

                html2pdf().set(opt).from(element).save().then(() => {
                    document.body.classList.remove('pdf-export-mode');
                    pdfExportBtn.innerHTML = originalBtnText;
                    pdfExportBtn.disabled = false;
                }).catch(err => {
                    console.error("PDF export failed:", err);
                    document.body.classList.remove('pdf-export-mode');
                    pdfExportBtn.innerHTML = "Export Failed";
                    setTimeout(() => { pdfExportBtn.innerHTML = originalBtnText; pdfExportBtn.disabled = false; }, 3000);
                });
            });
        }
        
        document.body.addEventListener('click', (event) => {
            if (event.target && event.target.matches('.export-btn')) {
                const header = event.target.closest('.table-header');
                if (header) {
                    const tableContainer = header.nextElementSibling;
                    const table = tableContainer ? tableContainer.querySelector('table') : null;
                    const titleElement = header.querySelector('h2, h3');
                    let title = titleElement ? titleElement.textContent.trim() : 'Export';
                    const safeFilename = title.replace(/[^a-z0-9_ -]/gi, '_').toLowerCase();
                    
                    if (table) {
                        exportTableToCSV(table, `${safeFilename}.csv`);
                    } else {
                        console.error("Could not find table to export for button:", event.target);
                    }
                }
            }
        });

        document.getElementById('expand-collapse-all')?.addEventListener('click', (e) => {
            const sections = document.querySelectorAll('.detailed-report-section.collapsible');
            const isExpanding = e.target.textContent.includes('Expand');
            sections.forEach(s => s.classList.toggle('collapsed', !isExpanding));
            e.target.textContent = isExpanding ? 'Collapse All' : 'Expand All';
        });
        
        document.querySelectorAll('.collapsible .section-title').forEach(title => title.addEventListener('click', (e) => {
            const section = e.currentTarget.closest('.collapsible');
            section.classList.toggle('collapsed');
            if (!section.classList.contains('collapsed')) {
                 section.querySelector('.section-content').style.display = '';
            }
        }));

        styleSummaryTable();
        
        const summaryTableBody = document.querySelector('#reports .summary-table tbody');
        if (summaryTableBody) {
            summaryTableBody.addEventListener('click', (e) => {
                const row = e.target.closest('tr');
                if (!row || !row.dataset.tcId) return;

                const targetId = row.dataset.tcId;
                const targetSection = document.querySelector(`.detailed-report-section[data-tc-id="${targetId}"]`);

                if (targetSection) {
                    targetSection.classList.remove('collapsed');
                    targetSection.scrollIntoView({ behavior: 'smooth', block: 'start' });
                    
                    targetSection.style.transition = 'background-color 0.5s ease-in-out';
                    targetSection.style.backgroundColor = 'rgba(54, 162, 235, 0.1)';
                    setTimeout(() => {
                        targetSection.style.backgroundColor = '';
                    }, 1500);
                }
            });
        }
        
        const revealObserver = new IntersectionObserver((entries) => {
            entries.forEach(entry => {
                if (entry.isIntersecting) {
                    entry.target.classList.add('visible');
                    revealObserver.unobserve(entry.target);
                }
            });
        }, { threshold: 0.1 });

        document.querySelectorAll('.section, .dashboard-overview, .chart-card, .kpi-grid').forEach(el => {
            el.classList.add('reveal');
            revealObserver.observe(el);
        });

    } catch (e) {
        console.error("A fatal error occurred during page initialization:", e);
        const mainContent = document.querySelector('.main-content');
        if(mainContent) mainContent.innerHTML = '<h1>A critical error occurred. Please check the browser console for details.</h1>';
    }
});
</script>


</body>
</html>


"""

SUMMARY_REPORT_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Data Quality Summary Report</title>
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Nunito:wght@400;600;700&display=swap');
        body { font-family: 'Nunito', sans-serif; background-color: #f4f7fa; color: #333; margin: 0; padding: 20px; }
        .container { max-width: 900px; margin: 0 auto; background-color: #ffffff; border-radius: 8px; box-shadow: 0 4px 15px rgba(0,0,0,0.05); overflow: hidden; }
        .header { background-color: #002060; color: #ffffff; padding: 30px; text-align: center; }
        .header h1 { margin: 0; font-size: 24px; }
        .header p { margin: 5px 0 0; font-size: 14px; opacity: 0.8; }
        .content { padding: 30px; }
        .content h2 { color: #002060; border-bottom: 2px solid #e9ecef; padding-bottom: 10px; margin-top: 0; }
        .kpi-grid { display: grid; grid-template-columns: repeat(2, 1fr); gap: 20px; margin-bottom: 30px; }
        .kpi-card { background-color: #f8f9fa; border: 1px solid #e9ecef; border-left: 5px solid #36a2eb; padding: 20px; border-radius: 5px; }
        .kpi-card h3 { margin: 0 0 5px; font-size: 14px; color: #6c757d; }
        .kpi-card p { margin: 0; font-size: 28px; font-weight: 700; color: #343a40; }
        .kpi-card.passed p { color: #28a745; }
        .kpi-card.failed p { color: #dc3545; }
        .kpi-card.passed { border-left-color: #28a745; }
        .kpi-card.failed { border-left-color: #dc3545; }
        .failures-table { width: 100%; border-collapse: collapse; font-size: 14px; table-layout: auto; }
        .failures-table th, .failures-table td { padding: 12px; text-align: left; border-bottom: 1px solid #e9ecef; }
        .failures-table th { background-color: #f8f9fa; font-weight: 700; }
        .failures-table .fail-count, .failures-table .numeric-val { font-weight: 700; color: #dc3545; text-align: center; }
        .failures-table .numeric-val { color: #343a40; }
        .no-failures { text-align: center; padding: 30px; background-color: #f8f9fa; border-radius: 5px; color: #6c757d; }
        .failures-table .fit-content {
            width: 1%;
            white-space: nowrap;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Data Quality Summary Report</h1>
            <p>Domain: {{ report_domain }} | Generated: {{ timestamp }}</p>
        </div>
        <div class="content">
            <h2>Overall Results</h2>
            <div class="kpi-grid">
                <div class="kpi-card"><h3>Total Scenarios</h3><p>{{ summary_stats.total }}</p></div>
                <div class="kpi-card"><h3>Total Differences</h3><p>{{ summary_stats.total_diffs }}</p></div>
                <div class="kpi-card passed"><h3>Scenarios Passed</h3><p>{{ summary_stats.success }}</p></div>
                <div class="kpi-card failed"><h3>Scenarios Failed</h3><p>{{ summary_stats.failure }}</p></div>
            </div>

            {% if top_failures %}
            <h2> Failing Rules</h2>
            <table class="failures-table">
                <thead>
                    <tr>
                        <th class="fit-content">TC#</th>
                        <th>Description</th>
                        <th class="fit-content" style="text-align: center;">Total Count</th>
                        <th class="fit-content" style="text-align: center;">Failure Count</th>
                        <th class="fit-content" style="text-align: center;">Failure %</th>
                    </tr>
                </thead>
                <tbody>
                    {% for failure in top_failures %}
                    <tr>
                        <td class="fit-content">{{ failure['TC#'] }}</td>
                        <td>{{ failure['Description'] }}</td>
                        <td class="numeric-val fit-content">{{ "{:,}".format(failure['Total_Count']) }}</td>
                        <td class="fail-count fit-content">{{ "{:,}".format(failure['Failure_Count']) }}</td>
                        <td class="fail-count fit-content">{{ failure['Failure_Percentage'] }}</td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
            {% else %}
            <h2>Failing Rules</h2>
            <div class="no-failures">
                <p>&#127881; Excellent! No failing rules were found.</p>
            </div>
            {% endif %}
        </div>
    </div>
</body>
</html>
"""

class Term:
    log_file = None

    @staticmethod
    def initialize_logging(filepath='Output.log'):
        Term.log_file = open(filepath, 'w', encoding='utf-8')

    @staticmethod
    def _log(message):
        if Term.log_file and not Term.log_file.closed:
            ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
            log_message = ansi_escape.sub('', message)
            Term.log_file.write(log_message + '\n')
            Term.log_file.flush()

    @staticmethod
    def header(title):
        formatted_title = f"\n{'='*80}\n| {title.center(76)} |\n{'='*80}"
        message = f"{Style.BRIGHT}{Fore.CYAN}{formatted_title}{Style.RESET_ALL}"
        print(message)
        Term._log(formatted_title)

    @staticmethod
    def scenario_header(title):
        formatted_title = f"\n>>> {title}"
        message = f"{Style.BRIGHT}{Fore.YELLOW}{formatted_title}{Style.RESET_ALL}"
        print(message)
        Term._log(formatted_title)

    @staticmethod
    def info(message):
        console_message = f"{Fore.GREEN}--> {message}"
        log_message = f"INFO: --> {message}"
        print(console_message)
        Term._log(log_message)

    @staticmethod
    def warn(message):
        console_message = f"{Fore.YELLOW}WARNING: {message}"
        log_message = f"WARNING: {message}"
        print(console_message)
        Term._log(log_message)

    @staticmethod
    def fatal(message):
        console_message = f"{Fore.RED}{Style.BRIGHT}FATAL: {message}"
        log_message = f"FATAL: {message}"
        print(console_message)
        if Term.log_file:
            Term.log_file.close()
        sys.exit(1)

    @staticmethod
    def close_log():
        if Term.log_file and not Term.log_file.closed:
            Term.log_file.close()

class ValidationEngine:
    def __init__(self, db_handler, config):
        self.db_handler = db_handler
        self.config = config
        self.max_failed_rows = config.getint('OUTPUT', 'max_failed_rows_export', fallback=1000)

    def _format_result(self, rule, status, count, msg, data, failing_col_list, query, runtime=0, total_count=0):
        result = rule.to_dict()
        if 'whereclause' in rule: 
             result['whereclause'] = rule['whereclause']
        result.update({
            'Status': status,
            'Failure_Count': count,
            'Total_Count': total_count,
            'Details': msg,
            'Failing_Data': data,
            'Failing_Column': failing_col_list,
            'SQL_Query': query,
            'Runtime': round(runtime, 2)
        })
        return result

    def _date_format_to_regex(self, format_string: str) -> str:
        format_map = {
            'YYYY': r'\\d{4}', 'YY': r'\\d{2}', 'MM': r'(0[1-9]|1[0-2])',
            'M': r'([1-9]|1[0-2])', 'DD': r'(0[1-9]|[12]\\d|3[01])', 'D': r'([1-9]|[12]\\d|3[01])',
        }
        pattern = re.escape(format_string.upper())
        for key, value in format_map.items():
            pattern = pattern.replace(key, value)
        return f'^{pattern}$'

    def run_check(self, rule: pd.Series):
        start_time = time()
        full_query_log = None
        failing_column_list = []
        total_count = 0

        scenario_upper = str(rule.get('Scenario', '')).strip().upper()
        
        # Extract Where Clause logic
        where_clause_input = str(rule.get('whereclause', '')).strip()
        # Handle empty/NaN values for whereclause
        if where_clause_input.lower() in ['nan', 'none', '', 'null']:
            where_clause_input = None
        
        if scenario_upper == 'FORMAT' and str(rule.get('Expected_Format', '')).strip().upper() == 'CUSTOM' and pd.isna(rule.get('Regex_Pattern')):
            msg = "Rule requires a value in the 'Regex_Pattern' column for this scenario."
            return self._format_result(rule, "SKIPPED", 0, msg, None, [rule.get('Column_Name')], None, time() - start_time)

        if scenario_upper in ['UNIQUENESS', 'DUPLICATE'] and pd.isna(rule.get('Column_Name')):
            msg = "Uniqueness check requires at least one column in 'Column_Name'."
            return self._format_result(rule, "SKIPPED", 0, msg, None, [], None, time() - start_time)

        try:
            # Note: _build_check_logic returns 'where_clause_input' but we have already extracted it above
            # passing the rule will handle building the core logic
            logic, failing_column_list, pk_cols_list, is_special_case, _ = self._build_check_logic(rule)

            if logic is None:
                return self._format_result(rule, "SKIPPED", 0, "Rule is incomplete or not applicable.", None, None, None, time() - start_time, total_count=0)

            if is_special_case:
                full_query_log = logic
                
                # ... special case logic (ROW_COUNT_MATCH and CUSTOM SQL) ...
                
                if str(rule.get('Expected_Format', '')).upper().strip() == 'ROW_COUNT_MATCH':
                    count_data = self.db_handler.execute_query_df(full_query_log)
                    if not count_data.empty:
                        src_count = count_data['SOURCE_COUNT'].iloc[0]
                        tgt_count = count_data['TARGET_COUNT'].iloc[0]
                        if src_count != tgt_count:
                            message = f"Row count mismatch. Source: {src_count}, Target: {tgt_count}."
                            actual_diff = abs(src_count - tgt_count)
                            return self._format_result(rule, "FAIL", actual_diff, message, count_data, failing_column_list, full_query_log, time() - start_time, total_count=src_count)
                        else:
                            message = f"Row counts match. Source: {src_count}, Target: {tgt_count}."
                            return self._format_result(rule, "PASS", 0, message, None, failing_column_list, full_query_log, time() - start_time, total_count=src_count)
                    else:
                        return self._format_result(rule, "ERROR", 0, "Could not retrieve row counts.", None, None, full_query_log, time() - start_time, total_count=0)
                
                elif str(rule.get('Scenario', '')).upper().strip() == 'CUSTOM SQL':
                    db, schema, table = rule.get('Target_Database'), rule.get('Target_Schema'), rule.get('Target_Table')
                    total_count = 0
                    if all([db, schema, table]):
                        full_table_name = f'"{db}"."{schema}"."{table}"'
                        # Calculate total count with Where Clause if present
                        if where_clause_input:
                            total_count_query = f'SELECT COUNT(*) FROM {full_table_name} WHERE {where_clause_input}'
                        else:
                            total_count_query = f'SELECT COUNT(*) FROM {full_table_name}'
                        total_count = self.db_handler.execute_query(total_count_query)[0][0]

                    # Wrap custom query to apply where clause if it's a simple SELECT
                    final_custom_query = full_query_log
                    if where_clause_input:
                        final_custom_query = f"SELECT * FROM ({full_query_log}) AS sub WHERE {where_clause_input}"

                    count_query = f"SELECT COUNT(*) FROM ({final_custom_query}) AS custom_query"
                    full_query_log = final_custom_query # Update log to reflect applied filter
                    
                    failure_count = self.db_handler.execute_query(count_query)[0][0]

                    if failure_count == 0:
                        return self._format_result(rule, "PASS", 0, "Custom query returned no rows.", None, None, full_query_log, time() - start_time, total_count=total_count)
                    
                    limited_query = f"SELECT * FROM ({final_custom_query}) AS custom_query LIMIT {self.max_failed_rows}"
                    failed_data = self.db_handler.execute_query_df(limited_query)
                    
                    message = f"Custom query returned {failure_count} rows."
                    status = "FAIL"
                    return self._format_result(rule, status, failure_count, message, failed_data, failing_column_list, full_query_log, time() - start_time, total_count=total_count)


            # Non-special case scenarios (all others: NULL, BOUNDARY, FORMAT, etc.)
            db, schema, table = rule['Target_Database'], rule['Target_Schema'], rule['Target_Table']
            full_table_name = f'"{db}"."{schema}"."{table}"'
            
            # 1. Calculate Total Count (Count of rows *in the filtered subset*)
            total_where = f"WHERE {where_clause_input}" if where_clause_input else ""
            total_count_query = f'SELECT COUNT(*) FROM {full_table_name} {total_where}'
            total_count = self.db_handler.execute_query(total_count_query)[0][0]
            
            # 2. Combine check logic AND whereclause for failure filter
            final_where_clause = logic
            if where_clause_input:
                final_where_clause = f"({logic}) AND ({where_clause_input})"
                
            count_query = f'SELECT COUNT(*) FROM {full_table_name} WHERE {final_where_clause}'
            full_query_log = count_query
            failure_count = self.db_handler.execute_query(count_query)[0][0]

            if total_count == 0:
                 return self._format_result(rule, "SKIPPED", 0, f"Total count in subset is 0 (check was run with {total_where}).", None, failing_column_list, full_query_log, time() - start_time, total_count=0)

            if failure_count == 0:
                return self._format_result(rule, "PASS", 0, "Check passed.", None, failing_column_list, count_query, time() - start_time, total_count=total_count)

            cols_to_select = []
            
            if not pk_cols_list:
                first_col = self.db_handler.get_first_column(db, schema, table)
                if first_col:
                    cols_to_select.append(first_col)

            cols_to_select.extend(pk_cols_list)
            cols_to_select.extend(failing_column_list)
            
            unique_cols_to_select = list(dict.fromkeys(cols_to_select))

            select_clause = ", ".join([f'"{c}"' for c in unique_cols_to_select]) if unique_cols_to_select else "*"
            
            order_by_clause = ""
            if failing_column_list:
                order_by_cols = ", ".join([f'"{c}"' for c in failing_column_list])
                order_by_clause = f"ORDER BY {order_by_cols}"

            details_query = f'SELECT {select_clause} FROM {full_table_name} WHERE {final_where_clause} {order_by_clause} LIMIT {self.max_failed_rows}'
            full_query_log = details_query
            failed_data = self.db_handler.execute_query_df(details_query)

            message = f"Check failed. Found {failure_count} non-compliant rows."
            return self._format_result(rule, "FAIL", failure_count, message, failed_data, failing_column_list, details_query, time() - start_time, total_count=total_count)

        except Exception as e:
            error_detail = f"Query execution failed: {e}\nGenerated Query: {full_query_log}"
            return self._format_result(rule, "ERROR", 0, error_detail, None, failing_column_list, full_query_log, time() - start_time, total_count=total_count)

    def _build_check_logic(self, rule: pd.Series) -> (Optional[str], List[str], List[str], bool, Optional[str]):
        """
        Builds the SQL logic for the check.
        Returns: logic_clause, failing_column_list_unquoted, pk_cols_list, is_special_case, where_clause_input
        """
        db, schema, table, column = rule['Target_Database'], rule['Target_Schema'], rule['Target_Table'], rule.get('Column_Name')
        pk_cols_list = self.db_handler.get_primary_keys(db, schema, table)
        logic_clause, is_special_case = None, False
        failing_column_list = [f'"{c.strip()}"' for c in str(column).split(',')] if pd.notna(column) else []
        full_table_name = f'"{db}"."{schema}"."{table}"'
        
        where_clause_input = str(rule.get('whereclause', '')).strip()
        where_clause_input = where_clause_input if where_clause_input and where_clause_input.lower() not in ['nan', 'none', 'null', ''] else None

        try:
            scenario = str(rule.get('Scenario', '')).strip().upper()
            expected_format = str(rule.get('Expected_Format', '')).strip().upper()
            
            column_sql = f'"{column}"' if column else ""
            if ',' in str(column):
                cols = [f'"{c.strip()}"' for c in str(column).split(',')]
                column_sql = ", ".join(cols)

            if scenario == 'NULL':
                if expected_format == 'NOT_NULL':
                    logic_clause = f'({column_sql} IS NULL OR CAST({column_sql} AS VARCHAR) = \'\')'
            
            elif scenario in ['UNIQUENESS', 'DUPLICATE']:
                cols_for_group_by = [f'"{c.strip()}"' for c in str(column).split(',')]
                group_by_clause = ", ".join(cols_for_group_by)
                
                # Apply whereclause to the subquery to find duplicates within the filtered set
                where_filter_subquery = f"WHERE {where_clause_input}" if where_clause_input else ""
                
                duplicate_subquery = f"SELECT {group_by_clause} FROM {full_table_name} {where_filter_subquery} GROUP BY {group_by_clause} HAVING COUNT(*) > 1"
                
                # The main logic is that the row's values are in the set of duplicates
                logic_clause = f"({group_by_clause}) IN ({duplicate_subquery})"
                # Note: The whereclause will be applied again in run_check, which is correct
                # for limiting the final result set to the filtered subset.

            elif scenario == 'BOUNDARY':
                if expected_format == 'NUMERIC':
                    min_val, max_val = rule['Boundary_Min'], rule['Boundary_Max']
                    logic_clause = f'({column_sql} < {min_val} OR {column_sql} > {max_val})'
                elif expected_format == 'NO_NEGATIVE':
                    logic_clause = f'{column_sql} < 0'
                elif expected_format == 'LENGTH':
                    min_len, max_len = int(rule['Boundary_Min']), int(rule['Boundary_Max'])
                    logic_clause = f'(LENGTH(CAST({column_sql} AS VARCHAR)) < {min_len} OR LENGTH(CAST({column_sql} AS VARCHAR)) > {max_len})'
            
            elif scenario == 'VALUE SET' and pd.notna(rule['Allowed_Values']):
                allowed_list = [f"'{str(val).strip()}'" for val in str(rule['Allowed_Values']).split(',')]
                logic_clause = f'{column_sql} NOT IN ({",".join(allowed_list)})'

            elif scenario == 'FORMAT':
                regex_pattern = None
                if expected_format == 'EMAIL': regex_pattern = r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,4}$'
                elif expected_format == 'NO_SPECIAL_CHARS': regex_pattern = r'^[a-zA-Z0-9_ ]*$'
                elif expected_format == 'PHONE': regex_pattern = r'^(\\d{10}|\\d{3}-\\d{3}-\\d{4})$'
                elif expected_format == 'ZIP': regex_pattern = r'^[0-9]{5}(-[0-9]{4})?$'
                elif expected_format == 'SSN': regex_pattern = r'^(\\d{9}|\\d{3}-\\d{2}-\\d{4})$'
                elif expected_format == 'DOB': regex_pattern = r'^(0[1-9]|1[0-2])-(0[1-9]|[12]\\d|3[01])-\\d{4}$'
                elif expected_format == 'NAME': regex_pattern = r"^[A-Za-zÀ-ÖØ-öø-ÿ'\s-]+$"
                elif expected_format == 'ADDRESS': regex_pattern = r"^[a-zA-Z0-9\s#\-.]*$"
                elif expected_format == 'CUSTOM' and pd.notna(rule['Regex_Pattern']): regex_pattern = str(rule['Regex_Pattern'])
                
                if regex_pattern:
                    safe_regex = regex_pattern.replace("'", "''").replace('\\', '\\\\')
                    logic_clause = f"NOT REGEXP_LIKE(CAST({column_sql} AS VARCHAR), '{safe_regex}')"
                elif expected_format == 'UPPERCASE': logic_clause = f'{column_sql} != UPPER({column_sql})'
                elif expected_format == 'LOWERCASE': logic_clause = f'{column_sql} != LOWER({column_sql})'
                elif expected_format == 'NO_WHITESPACE': logic_clause = f'{column_sql} != TRIM({column_sql})'
            
            elif scenario == 'DATE':
                if expected_format == 'NOT_IN_FUTURE': logic_clause = f'CAST({column_sql} AS DATE) > CURRENT_DATE()'
                elif expected_format == 'NOT_IN_PAST': logic_clause = f'CAST({column_sql} AS DATE) < CURRENT_DATE()'
                elif expected_format == 'FORMAT' and pd.notna(rule['Allowed_Values']):
                    date_regex = self._date_format_to_regex(str(rule['Allowed_Values']))
                    logic_clause = f"NOT REGEXP_LIKE(CAST({column_sql} AS VARCHAR), '{date_regex}')"
                elif 'IS_AFTER' in expected_format and ',' in str(column):
                    cols = [f'"{c.strip()}"' for c in str(column).split(',')]
                    col1, col2 = cols[0], cols[1]
                    logic_clause = f'CAST({col1} AS DATE) > CAST({col2} AS DATE)'
            
            elif scenario == 'RELATIONAL':
                is_special_case = True
                parent_db, parent_schema, parent_table = rule.get('Parent_Database', db), rule.get('Parent_Schema', schema), rule.get('Parent_Table')
                if expected_format == 'ROW_COUNT_MATCH' and pd.notna(parent_table):
                    full_parent_name = f'"{parent_db}"."{parent_schema}"."{parent_table}"'
                    failing_column_list = ['SOURCE_COUNT', 'TARGET_COUNT']
                    logic_clause = f"WITH s AS (SELECT COUNT(*) as ct FROM {full_table_name}), t AS (SELECT COUNT(*) as ct FROM {full_parent_name}) SELECT s.ct AS SOURCE_COUNT, t.ct AS TARGET_COUNT FROM s, t"
            
            elif scenario == 'DATA TYPE' and pd.notna(expected_format):
                logic_clause = f'TRY_CAST({column_sql} AS {expected_format}) IS NULL AND {column_sql} IS NOT NULL'
            
            elif scenario == 'CUSTOM SQL':
                is_special_case = True
                user_input = str(rule['Allowed_Values']).strip()
                if expected_format == 'BIG_QUERIES':
                    try:
                        sql_file_path = Path(user_input)
                        Term.info(f"Loading custom SQL from file: {sql_file_path}")
                        if not sql_file_path.is_file():
                            raise FileNotFoundError(f"The specified SQL file was not found: {sql_file_path}")
                        user_sql = sql_file_path.read_text(encoding='utf-8')
                    except Exception as e:
                        raise IOError(f"Failed to read SQL file '{user_input}'. Reason: {e}")
                else:
                    user_sql = user_input

                if user_sql.strip().endswith(';'):
                    user_sql = user_sql.strip()[:-1]
                logic_clause = user_sql

        except (KeyError, TypeError, IndexError) as e:
            return None, [], [], False, None
        
        failing_column_list_unquoted = [c.replace('"', '') for c in failing_column_list]
        return logic_clause, failing_column_list_unquoted, pk_cols_list, is_special_case, where_clause_input

def get_unique_filepath(directory, filename):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    
    filepath = directory / filename
    counter = 1
    
    while filepath.exists():
        try:
            with open(filepath, 'a'): pass
            break 
        except (PermissionError, IOError):
            Term.warn(f"'{filepath.name}' is open or locked. Trying a new filename.")
            new_name = f"{filepath.stem}_{counter}{filepath.suffix}"
            filepath = directory / new_name
            counter += 1
            
    return str(filepath)

class ReportGenerator:
    def __init__(self, results, pk_info, config, profiling_results=None, profiling_enabled=False, dynamic_output_dir=None, tc_name=None, enable_visual_report=True, enable_scenario_report=True, enable_defects_tab=True, enable_defect_flow=False):
        self.results = results
        self.pk_info = pk_info
        self.config = config
        self.db_type = config.get('DATABASE', 'selected_db', fallback='N/A')
        self.timestamp = datetime.now()
        self.graph_type = config.get('GRAPHS', 'Type', fallback='bars').lower()
        self.enable_dimension_analysis = config.getboolean('GRAPHS', 'enable_dimension_analysis_chart', fallback=False)
        self.enable_rows_by_dimension_pie = config.getboolean('GRAPHS', 'enable_rows_by_dimension_pie', fallback=True)

        self.profiling_results = profiling_results if profiling_results is not None else []
        self.profiling_enabled = profiling_enabled
        self.enable_visual_report = enable_visual_report
        self.enable_scenario_report = enable_scenario_report
        self.enable_defects_tab = enable_defects_tab
        self.enable_defect_flow = enable_defect_flow
        self.show_company_logo = config.get('OUTPUT', 'Company_Logo', fallback='yes').lower() == 'yes'


        if dynamic_output_dir:
            output_dir = dynamic_output_dir
        else:
            output_dir = self.config.get('OUTPUT', 'output_directory', fallback='.')

        date_dir = self.timestamp.strftime('%Y-%m-%d')
        month_dir = self.timestamp.strftime('%B')
        self.final_dir = Path(output_dir) / month_dir / date_dir

        if tc_name:
            safe_tc_name = re.sub(r'[<>:"/\\|?*]', '_', tc_name)
            excel_filename = f"{safe_tc_name}.xlsx"
            html_filename = f"{safe_tc_name}.html"
            summary_filename = f"{safe_tc_name}_Summary.html"
        else:
            excel_filename = self.config.get('OUTPUT', 'excel_report_filename', fallback='QualityReport.xlsx')
            default_html_filename = f"{Path(excel_filename).stem}.html"
            html_filename = self.config.get('OUTPUT', 'html_report_filename', fallback=default_html_filename)
            summary_filename = self.config.get('OUTPUT', 'summary_html_filename', fallback='Emailable_Summary.html')

        self.excel_filepath = get_unique_filepath(self.final_dir, excel_filename)
        self.html_filepath = get_unique_filepath(self.final_dir, html_filename)
        self.summary_filepath = get_unique_filepath(self.final_dir, summary_filename)

    def generate_reports(self):
        Term.header("GENERATING REPORTS")
        self.generate_excel()
        html_path = None
        if self.config.get('OUTPUT', 'report_html', fallback='yes').lower() == 'yes':
            html_path = self.generate_html()
            self.generate_summary_email()
        return self.excel_filepath, html_path

    def generate_excel(self):
        Term.info(f"Generating Excel report to: {self.excel_filepath}")
        
        summary_df = pd.DataFrame(self.results)
        pk_df = pd.DataFrame(self.pk_info)

        if 'Total_Count' not in summary_df.columns: summary_df['Total_Count'] = 0
        summary_df['Total_Count'] = pd.to_numeric(summary_df['Total_Count'], errors='coerce').fillna(0).astype(int)
        summary_df['Failure_Count'] = pd.to_numeric(summary_df['Failure_Count'], errors='coerce').fillna(0).astype(int)
        
        percentage = pd.Series(index=summary_df.index, dtype=float)
        valid_mask = summary_df['Total_Count'] > 0
        percentage[valid_mask] = (summary_df['Failure_Count'][valid_mask] / summary_df['Total_Count'][valid_mask]) * 100
        summary_df['Failure Percentage'] = percentage.map('{:.2f}%'.format).fillna('N/A')
        
        with pd.ExcelWriter(self.excel_filepath, engine='xlsxwriter') as writer:
            workbook = writer.book
            
            header_format = workbook.add_format({'bold': True, 'font_color': '#FFFFFF', 'fg_color': '#002060', 'border': 1, 'valign': 'top', 'text_wrap': True})
            pass_format = workbook.add_format({'bg_color': '#C6EFCE', 'font_color': '#006100'})
            fail_format = workbook.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'})
            warn_format = workbook.add_format({'bg_color': '#FFEB9C', 'font_color': '#9C6500'})
            fail_record_format = workbook.add_format({'bg_color': '#FFC7CE'})
            query_header_format = workbook.add_format({'bold': True, 'font_color': '#002060'})
            query_text_format = workbook.add_format({'text_wrap': True, 'valign': 'top', 'font_name': 'Consolas'})
            
            default_summary_cols = [
                'TC#', 'Domain', 'Dimension_Matrix', 'Test_Case', 'Description', 'Run', 'Scenario', 
                'Expected_Format', 'Target_Database', 'Target_Schema', 'Target_Table', 'Column_Name', 
                'Allowed_Values', 'Regex_Pattern', 'Boundary_Min', 'Boundary_Max', 'whereclause', 'Status', 
                'Total_Count', 'Failure_Count', 'Failure_Percentage', 'Details', 'SQL_Query'
            ]

             
            try:
                cols_from_config = self.config.get('REPORT_COLUMNS', 'summary_excel_columns')
                 
                summary_cols_ordered = [col.strip() for col in cols_from_config.split(',')]
                Term.info("Using custom column list for Excel summary from config.ini.")
            except (configparser.NoSectionError, configparser.NoOptionError):
                 
                Term.info("No custom Excel columns defined in config.ini. Using default full list.")
                summary_cols_ordered = default_summary_cols

             

            final_summary_df = summary_df[[c for c in summary_cols_ordered if c in summary_df.columns]].copy()
            final_summary_df.to_excel(writer, sheet_name='Summary', index=False)
            ws_summary = writer.sheets['Summary']
            
            for col_num, value in enumerate(final_summary_df.columns.values):
                ws_summary.write(0, col_num, value, header_format)

            status_col_letter = chr(ord('A') + final_summary_df.columns.get_loc('Status'))
            ws_summary.conditional_format(1, 0, len(final_summary_df), len(final_summary_df.columns) - 1, {'type': 'formula', 'criteria': f'=${status_col_letter}2="PASS"', 'format': pass_format})
            ws_summary.conditional_format(1, 0, len(final_summary_df), len(final_summary_df.columns) - 1, {'type': 'formula', 'criteria': f'=${status_col_letter}2="FAIL"', 'format': fail_format})
            ws_summary.conditional_format(1, 0, len(final_summary_df), len(final_summary_df.columns) - 1, {'type': 'formula', 'criteria': f'=AND(${status_col_letter}2<>"PASS", ${status_col_letter}2<>"FAIL")', 'format': warn_format})
            ws_summary.autofit()

            if not pk_df.empty:
                pk_df.to_excel(writer, sheet_name='PK_Discovery', index=False)
                ws_pk = writer.sheets['PK_Discovery']
                for col_num, value in enumerate(pk_df.columns.values): ws_pk.write(0, col_num, value, header_format)
                ws_pk.autofit()

            failed_results = [r for r in self.results if r.get('Status') in ['FAIL', 'ERROR'] and r.get('Failing_Data') is not None]
            used_sheet_names = set()
            
            include_sql = self.config.getboolean('OUTPUT', 'include_sql_in_failures', fallback=False)
            
            for result in failed_results:
                failing_df = result.get('Failing_Data')
                if isinstance(failing_df, pd.DataFrame) and not failing_df.empty:
                    base_name = f"{result.get('TC#', 'TC')} - {result.get('Scenario', 'Scenario')}"
                    safe_name = re.sub(r'[\\/*?:\[\]]', '_', base_name)[:31]
                    
                    sheet_name = safe_name
                    counter = 1
                    while sheet_name in used_sheet_names:
                        suffix = f'_{counter}'; sheet_name = f'{safe_name[:31-len(suffix)]}{suffix}'; counter += 1
                    used_sheet_names.add(sheet_name)
                    
                    failing_df.to_excel(writer, sheet_name=sheet_name, index=False)
                    ws_fail = writer.sheets[sheet_name]
                    
                    for col_num, value in enumerate(failing_df.columns.values): ws_fail.write(0, col_num, value, header_format)
                    ws_fail.conditional_format(1, 0, len(failing_df), len(failing_df.columns) - 1, {'type': 'no_blanks', 'format': fail_record_format})
                    ws_fail.autofit()

                    if include_sql:
                        sql_query = result.get('SQL_Query', 'Query not available.')
                        query_header_row = len(failing_df) + 2; query_text_row = query_header_row + 1
                        ws_fail.write(query_header_row, 0, "SQL Query Used:", query_header_format)
                        if len(failing_df.columns) > 1: ws_fail.merge_range(query_text_row, 0, query_text_row, len(failing_df.columns) - 1, sql_query, query_text_format)
                        else: ws_fail.write(query_text_row, 0, sql_query, query_text_format)
                        lines = sql_query.count('\n') + (len(sql_query) // 100) + 2; row_height = min(400, lines * 15)
                        ws_fail.set_row(query_text_row, row_height); ws_fail.set_column(0, 0, 100)

        Term.info("Excel report generation complete.")

    def generate_summary_email(self):
        Term.info("Attempting to generate Emailable Summary report...")
        try:
            if Environment is None:
                raise ImportError("Jinja2 is not installed.")

            results_df = pd.DataFrame(self.results)
            results_df['Failure_Count'] = pd.to_numeric(results_df['Failure_Count'], errors='coerce').fillna(0).astype(int)
            results_df['Total_Count'] = pd.to_numeric(results_df['Total_Count'], errors='coerce').fillna(0).astype(int)
            
            percentage = pd.Series(index=results_df.index, dtype=float)
            valid_mask = results_df['Total_Count'] > 0
            percentage[valid_mask] = (results_df['Failure_Count'][valid_mask] / results_df['Total_Count'][valid_mask]) * 100
            results_df['Failure_Percentage'] = percentage.map('{:.2f}%'.format).fillna('N/A')

            total = len(results_df)
            success = len(results_df[results_df['Status'] == 'PASS'])
            total_diffs = results_df['Failure_Count'].sum()
            summary_stats = {'total': total, 'success': success, 'failure': total - success, 'total_diffs': f"{int(total_diffs):,}"}

            failed_df = results_df[results_df['Status'] == 'FAIL'].copy()
            top_failures = []
            
            required_cols = ['TC#', 'Description', 'Failure_Count', 'Total_Count', 'Failure_Percentage']
            
            if not failed_df.empty and all(col in failed_df.columns for col in required_cols):
                if 'Dimension_Matrix' in failed_df.columns and 'Description' in failed_df.columns:
                    failed_df['Description'] = failed_df.apply(
                        lambda row: f"{row.get('Dimension_Matrix', 'N/A')} : {row.get('Description', 'No Description')}",
                        axis=1
                    )
                
                top_failures_df = failed_df.nlargest(10, 'Failure_Count')
                top_failures = top_failures_df[required_cols].to_dict('records')

            env = Environment()
            template = env.from_string(SUMMARY_REPORT_TEMPLATE)

            html_content = template.render(
                report_domain=results_df['Domain'].mode()[0] if 'Domain' in results_df.columns and not results_df['Domain'].empty else 'General',
                timestamp=self.timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                summary_stats=summary_stats,
                top_failures=top_failures
            )
            
            with open(self.summary_filepath, 'w', encoding='utf-8') as f:
                f.write(html_content)
            
            Term.info(f"Successfully generated Emailable Summary to: {self.summary_filepath}")

        except Exception as e:
            Term.warn(f"Could not generate Emailable Summary report. Error: {e}")
            tb_str = traceback.format_exc()
            Term._log("\n--- SUMMARY HTML GENERATION ERROR ---\n" + tb_str + "\n-----------------------------\n")

    def generate_html(self):
        Term.info(f"Attempting to generate HTML report...")
        try:
            if Environment is None: raise ImportError("Jinja2 is not installed.")
            results_df = pd.DataFrame(self.results)
            if 'Total_Count' not in results_df.columns: results_df['Total_Count'] = 0
            results_df['Total_Count'] = pd.to_numeric(results_df['Total_Count'], errors='coerce').fillna(0).astype(int)
            results_df['Failure_Count'] = pd.to_numeric(results_df['Failure_Count'], errors='coerce').fillna(0).astype(int)

            percentage = pd.Series(index=results_df.index, dtype=float)
            valid_mask = results_df['Total_Count'] > 0
            percentage[valid_mask] = (results_df['Failure_Count'][valid_mask] / results_df['Total_Count'][valid_mask]) * 100
            results_df['Failure_Percentage'] = percentage.map('{:.2f}%'.format).fillna('N/A')

            total = len(results_df); success = len(results_df[results_df['Status'] == 'PASS']); total_diffs = results_df['Failure_Count'].sum()
            
            total_rows_checked = results_df['Total_Count'].sum()

            total_unique_rows_sum = 0
            if 'Total_Count' in results_df.columns and 'Target_Table' in results_df.columns:
                try:
                    table_counts = results_df.groupby(['Target_Database', 'Target_Schema', 'Target_Table'])['Total_Count'].max()
                    total_unique_rows_sum = table_counts.sum()
                except Exception as e:
                    Term.warn(f"Could not calculate Total Unique Table Row Count: {e}")
            
            total_tables = total_unique_rows_sum
            
            # --- UPDATED LOGIC FOR SUCCESS PERCENTAGE ---
            # Old logic (Negative possibility): (total_tables - total_diffs) / total_tables
            # New logic (Rule Adherence Rate): (total_rows_checked - total_diffs) / total_rows_checked
            
            if total_rows_checked > 0:
                # We subtract failures from total CHECKS, not distinct rows.
                successful_checks = int(total_rows_checked) - int(total_diffs)
                success_percentage = (successful_checks / total_rows_checked) * 100
                if success_percentage < 0: success_percentage = 0 # Safety net
            else:
                success_percentage = 0

            #if 'Dimension_Matrix' in results_df.columns:
            #    results_df['Dimension_Matrix'] = results_df['Dimension_Matrix'].fillna('N/A')
            #    total_distinct_columns = results_df['Dimension_Matrix'].nunique()
            #else:
            #     total_distinct_columns = 0
            
            if 'Column_Name' in results_df.columns:
                total_distinct_columns = results_df['Column_Name'].astype(str).nunique()
            else:
                 total_distinct_columns = 0
                 
            summary_stats = {
                'total': total, 
                'success': success, 
                'failure': total - success, 
                'total_diffs': f"{int(total_diffs):,}",
                'total_rows_checked': f"{int(total_rows_checked):,}",
                'total_distinct_columns': f"{total_distinct_columns:,}",
                'total_tables': f"{int(total_tables):,}",
                'success_percentage': f"{success_percentage:.1f}%"
            }

            report_domain = results_df['Domain'].mode()[0] if 'Domain' in results_df.columns and not results_df['Domain'].empty else 'General'
            
            report_subject_area = 'General'
            if 'Subject_Area' in results_df.columns and not results_df['Subject_Area'].dropna().empty:
                try:
                    report_subject_area = results_df['Subject_Area'].mode()[0]
                except Exception:
                    report_subject_area = results_df['Subject_Area'].iloc[0]
                    
                    
            summary_display_cols = {'TC#': 'TC#', 'Domain': 'Domain', 'Dimension_Matrix': 'Dimension_Matrix', 'Scenario': 'Scenario', 'Description': 'Description',  'Status': 'Overall_Status', 'Total_Count': 'Total Rows Checked', 'Failure_Count': 'Difference Found', 'Failure_Percentage': 'Failure %', 'Runtime': 'Runtime(sec)'}
            existing_cols = [col for col in summary_display_cols.keys() if col in results_df.columns]
            summary_table_df = results_df[existing_cols].rename(columns=summary_display_cols)
            summary_table_html = summary_table_df.to_html(classes='summary-table', index=False, border=0)
            
            detailed_reports = []
            failed_results_df = results_df[results_df["Status"].isin(["FAIL", "ERROR", "TIMEOUT"])].copy()

            for _, row in results_df.iterrows():
                summary_dict = row.to_dict()
                report_title = f"{summary_dict.get('TC#', 'N/A')} - {summary_dict.get('Dimension_Matrix', 'N/A')} - {summary_dict.get('Scenario', 'N/A')}: {summary_dict.get('Description', 'No Description')}"
                
                full_target_name = f"{summary_dict.get('Target_Database', '')}.{summary_dict.get('Target_Schema', '')}.{summary_dict.get('Target_Table', 'N/A')}"

                summary_dict.update({
                    'Report_Title': report_title, 
                    'Comparison': report_title, 
                    'Target Table': summary_dict.get('Target_Table', 'N/A'),
                    'Full_Target_Name': full_target_name
                })

                report_dict = {'summary': summary_dict, 'dataframes': {}}
                failing_df = row.get("Failing_Data")
                if isinstance(failing_df, pd.DataFrame) and not failing_df.empty:
                    failing_df.index = failing_df.index + 1
                    styler = failing_df.style.set_table_attributes('class="details-table"').format(formatter=lambda v: f"{v:.0f}" if isinstance(v, float) and v.is_integer() else f"{v:.2f}" if isinstance(v, float) else v )
                    
                    fail_title = "Failing Records"
                    report_dict['dataframes'][fail_title] = styler.to_html(index=True)
                detailed_reports.append(report_dict)

            defect_analytics, defect_kpis = {}, {}; defect_table_html = "<p>No defects were found during the comparison.</p>"
            if not failed_results_df.empty and 'Failure_Count' in failed_results_df.columns:
                for col in ['Target_Table', 'Column_Name', 'Scenario', 'Dimension_Matrix']:
                    if col not in failed_results_df.columns: failed_results_df[col] = 'N/A'
                failed_results_df.fillna({'Column_Name': 'N/A', 'Target_Table': 'N/A', 'Scenario': 'N/A', 'Dimension_Matrix': 'N/A'}, inplace=True)
                total_defects = failed_results_df['Failure_Count'].sum(); percent_rules_failed = (len(failed_results_df) / len(results_df)) * 100 if len(results_df) > 0 else 0
                defect_kpis = {'total_defects': f"{int(total_defects):,}", 'most_problematic_table': failed_results_df.groupby('Target_Table')['Failure_Count'].sum().idxmax() if total_defects > 0 else 'N/A', 'top_failing_scenario': failed_results_df.groupby('Scenario')['Failure_Count'].sum().idxmax() if total_defects > 0 else 'N/A', 'percent_rules_failed': f"{percent_rules_failed:.1f}%"}
                def get_defect_type(row):
                    if row['Status'] == 'ERROR': return 'Execution Error'
                    if row['Status'] == 'TIMEOUT': return 'Timeout'
                    return 'Value Mismatch'
                failed_results_df['Defect_Type'] = failed_results_df.apply(get_defect_type, axis=1)
                reason_counts = failed_results_df['Defect_Type'].value_counts()
                defect_analytics['by_reason'] = {'labels': reason_counts.index.tolist(), 'data': reason_counts.values.tolist()}
                for group_key in ['Scenario', 'Table']:
                    counts = failed_results_df.groupby(f'Target_{group_key}' if group_key == 'Table' else group_key)['Failure_Count'].sum().nlargest(10)
                    defect_analytics[f'by_{group_key.lower()}'] = {'labels': counts.index.tolist(), 'data': counts.values.tolist()} if not counts.empty else {}
                hotspot_df = failed_results_df.groupby(['Target_Table', 'Column_Name'])['Failure_Count'].sum().reset_index(); hotspot_df['label'] = hotspot_df['Target_Table'] + '.' + hotspot_df['Column_Name']; hotspot_df = hotspot_df.nlargest(10, 'Failure_Count')
                defect_analytics['hotspots'] = {'labels': hotspot_df['label'].tolist(), 'data': hotspot_df['Failure_Count'].values.tolist()} if not hotspot_df.empty else {}
                
                treemap_df = failed_results_df[failed_results_df['Failure_Count'] > 0].copy()
                
                mask = treemap_df['Column_Name'].isnull() | (treemap_df['Column_Name'] == 'N/A') | (treemap_df['Column_Name'].str.strip() == '')
                treemap_df.loc[mask, 'Column_Name'] = treemap_df.loc[mask, 'Scenario']
                treemap_df['Column_Name'] = treemap_df['Column_Name'].fillna('Table-Level Check')
                
                defect_analytics['treemap_data'] = treemap_df.groupby(['Target_Table', 'Column_Name'])['Failure_Count'].sum().reset_index().to_dict('records') if not treemap_df.empty else []
                defect_cols = ['TC#', 'Scenario','Dimension_Matrix', 'Description', 'Target_Table', 'Column_Name', 'Defect_Type', 'Total_Count', 'Failure_Count', 'Failure_Percentage', 'Details']
                existing_defect_cols = [col for col in defect_cols if col in failed_results_df.columns]
                defect_details_df = failed_results_df[existing_defect_cols].rename(columns={'Target_Table': 'Table', 'Column_Name': 'Failing Column', 'Defect_Type': 'Reason', 'Total_Count': 'Total Count', 'Failure_Count': 'Failure Count', 'Failure_Percentage': 'Percentage'})
                defect_table_html = defect_details_df.to_html(classes='details-table summary-table', index=False, border=0)

                if self.enable_defect_flow:
                    sankey_data = []
                    if 'Dimension_Matrix' in failed_results_df.columns:
                        failed_results_df['Sankey_Column_Name'] = failed_results_df.apply(
                            lambda row: f"{row['Target_Table']}.{row['Column_Name']}", axis=1
                        )
                        
                        flow1 = failed_results_df.groupby(['Dimension_Matrix', 'Target_Table'])['Failure_Count'].sum().reset_index()
                        for _, row in flow1.iterrows():
                            sankey_data.append({'from': row['Dimension_Matrix'], 'to': row['Target_Table'], 'flow': int(row['Failure_Count'])})

                        flow2 = failed_results_df.groupby(['Target_Table', 'Sankey_Column_Name'])['Failure_Count'].sum().reset_index()
                        for _, row in flow2.iterrows():
                            sankey_data.append({'from': row['Target_Table'], 'to': row['Sankey_Column_Name'], 'flow': int(row['Failure_Count'])})
                    
                    defect_analytics['sankey_data'] = sankey_data
                else:
                    defect_analytics['sankey_data'] = []
                    
            scenario_chart_data = []
                                                                    
            if 'Scenario' in results_df.columns:
                def get_row_counts(row):
                    total, failed = row['Total_Count'], row['Failure_Count']
                    matched = max(0, total - failed)
                    mismatched = failed
                    if row['Status'].upper() == 'PASS':
                        mismatched = 0
                        matched = total
                    return int(matched), int(mismatched)
                    
                results_df[['Matched_Rows', 'Mismatched_Rows']] = results_df.apply(
                    lambda row: pd.Series(get_row_counts(row)), axis=1
                )
                
                for name, group in results_df.groupby('Scenario'):
                    matched, mismatched = group['Matched_Rows'].sum(), group['Mismatched_Rows'].sum()
                    scenario_chart_data.append({
                        'total_compared': f"{int(matched + mismatched):,}", 
                        'matched': int(matched), 
                        'mismatched': int(mismatched), 
                        'scenario_name': name
                    })

            dimension_matrix_chart_data = {}
            if 'Dimension_Matrix' in results_df.columns:
                results_df['Dimension_Matrix'] = results_df['Dimension_Matrix'].fillna('N/A'); dimension_groups = results_df.groupby('Dimension_Matrix')
                labels, pass_counts, fail_counts, total_records_list, failure_percentage_list, total_failures_list = [], [], [], [], [], []
                for name, group in dimension_groups:
                    labels.append(name)
                    pass_count = (group['Status'] == 'PASS').sum()
                    fail_count = len(group) - pass_count
                    pass_counts.append(pass_count)
                    fail_counts.append(fail_count)
                    total_records = group['Total_Count'].sum(); total_failures = group['Failure_Count'].sum(); total_records_list.append(int(total_records)); total_failures_list.append(int(total_failures))
                    if total_records > 0: failure_percentage = (total_failures / total_records) * 100; failure_percentage_list.append(round(failure_percentage, 2))
                    else: failure_percentage_list.append(0)
                dimension_matrix_chart_data = {'labels': labels, 'pass_counts': [int(c) for c in pass_counts], 'fail_counts': [int(c) for c in fail_counts], 'total_records': total_records_list, 'failure_percentages': failure_percentage_list, 'total_failures': total_failures_list}
            
            dimension_matrix_chart_data_json = json.dumps(dimension_matrix_chart_data); scenario_chart_data_json = json.dumps(scenario_chart_data); defect_analytics_json = json.dumps(defect_analytics)
            
            env = Environment(); env.filters['fromjson'] = json.loads
            template = env.from_string(FULL_REPORT_TEMPLATE)
            
            html_content = template.render(
                platform_name=self.db_type.upper(),
                timestamp=self.timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                summary_stats=summary_stats,
                summary_table_html=summary_table_html,
                detailed_reports=detailed_reports,
                defect_analytics_json=defect_analytics_json,
                defect_kpis=defect_kpis,
                defect_table_html=defect_table_html,
                source_name='Source DB',
                target_name=self.db_type.upper(),
                source_status='Connected',
                target_status='Connected',
                scenario_chart_data_json=scenario_chart_data_json,
                dimension_matrix_chart_data_json=dimension_matrix_chart_data_json,
                report_domain=report_domain,
                report_subjectarea=report_subject_area, 
                profiling_results=self.profiling_results,
                profiling_enabled=self.profiling_enabled,
                enable_visual_report=self.enable_visual_report,
                enable_scenario_report=self.enable_scenario_report,
                enable_defects_tab=self.enable_defects_tab,
                enable_defect_flow=self.enable_defect_flow,
                show_company_logo=self.show_company_logo,
                graph_type=self.graph_type,
                enable_dimension_analysis=self.enable_dimension_analysis,
                enable_rows_by_dimension_pie=self.enable_rows_by_dimension_pie
            )
            with open(self.html_filepath, 'w', encoding='utf-8') as f: f.write(html_content)
            Term.info(f"Successfully generated HTML report to: {self.html_filepath}")
            return self.html_filepath
        except ImportError: Term.warn("Jinja2 is not installed. Skipping HTML report. Please run: pip install Jinja2")
        except Exception:
            Term.warn(f"Could not generate HTML report due to an unexpected error. Please check details in Output.log")
            tb_str = traceback.format_exc(); print(tb_str); Term._log("\n--- HTML GENERATION ERROR ---\n" + tb_str + "\n-----------------------------\n")
        return None

class BaseDBHandler:
    def __init__(self, config, db_key):
        self.config = config
        self.db_key = db_key
        self.connection = None
        self.column_cache = {}

    def connect(self):
        raise NotImplementedError

    def disconnect(self):
        if self.connection:
            try:
                self.connection.close()
            except Exception as e:
                print(f"Notice: Could not cleanly disconnect from database: {e}")

    def execute_query(self, query):
        # NOTE: For non-SELECT queries, execute_query might need a commit/autocommit depending on the DB
        # For data quality checks, this is mostly for scalar COUNT results
        with self.connection.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall()

    def execute_query_df(self, query):
        # Using read_sql which implicitly handles connection and data retrieval for DataFrame
        return pd.read_sql(query, self.connection)

    def get_primary_keys(self, database, schema, table):
        raise NotImplementedError

    def _fetch_table_columns(self, database, schema, table):
        raise NotImplementedError

    def get_table_columns(self, database, schema, table):
        cache_key = f'"{database}"."{schema}"."{table}"'
        if cache_key not in self.column_cache:
            self.column_cache[cache_key] = self._fetch_table_columns(database, schema, table)
        return self.column_cache[cache_key]

    def get_first_column(self, database, schema, table):
        columns = self.get_table_columns(database, schema, table)
        return columns[0] if columns else None
    
    def get_detailed_column_metadata(self, database, schema, table) -> List[Dict[str, Any]]:
        raise NotImplementedError

    def get_foreign_keys(self, database, schema, table) -> List[Dict[str, str]]:
        raise NotImplementedError
    
    def list_tables_in_schema(self, database, schema) -> List[str]:
        raise NotImplementedError

    def expand_table_list(self, targets: List[str]) -> List[tuple]:
        expanded_list = []
        for target in targets:
            parts = target.split('.')
            if len(parts) != 3:
                Term.warn(f"Invalid target format '{target}'. Skipping. Use 'DB.SCHEMA.TABLE' or 'DB.SCHEMA.*'.")
                continue
            db, schema, table = parts
            if table == '*':
                try:
                    tables_in_schema = self.list_tables_in_schema(db, schema)
                    for tbl in tables_in_schema:
                        expanded_list.append((db, schema, tbl))
                except Exception as e:
                    Term.warn(f"Could not list tables for '{db}.{schema}'. Error: {e}")
            else:
                expanded_list.append((db, schema, table))
        return expanded_list

class SnowflakeHandler(BaseDBHandler):
    def connect(self):
        db_config = self.config[self.db_key]
        connection_params = {'user': db_config.get('user'), 'account': db_config.get('account'), 'warehouse': db_config.get('warehouse'), 'database': db_config.get('database'), 'role': db_config.get('role')}
        if db_config.get('authenticator', '').lower() == 'externalbrowser': connection_params['authenticator'] = 'externalbrowser'
        else: connection_params['password'] = db_config.get('password')
        self.connection = snowflake.connector.connect(**{k: v for k, v in connection_params.items() if v is not None})

    def disconnect(self):
        if self.connection and not self.connection.is_closed(): self.connection.close()

    def get_primary_keys(self, database, schema, table):
        query = f"SHOW PRIMARY KEYS IN TABLE \"{database}\".\"{schema}\".\"{table}\";"
        try:
            df = self.execute_query_df(query)
            if not df.empty and 'column_name' in df.columns: return df['column_name'].tolist()
        except Exception: return []
        return []

    def _fetch_table_columns(self, database, schema, table):
        query = f'SHOW COLUMNS IN TABLE "{database}"."{schema}"."{table}";'
        try:
            df = self.execute_query_df(query)
            if not df.empty and 'column_name' in df.columns: return df['column_name'].tolist()
        except Exception: return []
        return []

    def list_tables_in_schema(self, database, schema):
        query = f'SHOW TABLES IN SCHEMA "{database}"."{schema}";'
        df = self.execute_query_df(query)
        if not df.empty and 'name' in df.columns:
            return df['name'].tolist()
        return []

    def get_detailed_column_metadata(self, database, schema, table):
        query = f'SHOW COLUMNS IN TABLE "{database}"."{schema}"."{table}";'
        df = self.execute_query_df(query)
        if not df.empty:
            df = df.rename(columns={'column_name': 'column_name', 'data_type': 'data_type', 'null?': 'is_nullable'})
            df['is_nullable'] = df['is_nullable'].apply(lambda x: 'YES' if x == 'Y' else 'NO')
            
            def parse_sf_type(type_str):
                try:
                    type_info = json.loads(type_str)
                    return type_info.get('type'), type_info.get('length')
                except (json.JSONDecodeError, TypeError):
                    return None, None
            
            parsed_types = df['data_type'].apply(parse_sf_type)
            df['data_type_name'] = parsed_types.apply(lambda x: x[0])
            df['character_maximum_length'] = parsed_types.apply(lambda x: x[1])

            pk_query = f'SHOW PRIMARY KEYS IN TABLE "{database}"."{schema}"."{table}";'
            try:
                pk_df = self.execute_query_df(pk_query)
                if not pk_df.empty:
                    pk_cols = pk_df['column_name'].tolist()
                    df['pk_name'] = df['column_name'].apply(lambda c: 'primary_key' if c in pk_cols else None)
                else:
                    df['pk_name'] = None
            except Exception:
                 df['pk_name'] = None

            return df.to_dict('records')
        return []

    def get_foreign_keys(self, database, schema, table):
        query = f'SHOW IMPORTED KEYS IN TABLE "{database}"."{schema}"."{table}";'
        try:
            df = self.execute_query_df(query)
            if not df.empty:
                df = df.rename(columns={
                    'fk_column_name': 'column_name',
                    'pk_schema_name': 'parent_schema',
                    'pk_table_name': 'parent_table',
                    'pk_column_name': 'parent_column'
                })
                return df[['column_name', 'parent_schema', 'parent_table', 'parent_column']].to_dict('records')
        except Exception as e:
            Term.warn(f"Could not retrieve foreign keys for {table}. This may be a permissions issue. Error: {e}")
        return []

class PostgresHandler(BaseDBHandler):
    def connect(self):
        db_config = dict(self.config.items(self.db_key))
        self.connection = psycopg2.connect(**db_config)
    def disconnect(self):
        if self.connection and not self.connection.closed: self.connection.close()
    def get_primary_keys(self, database, schema, table):
        query = f"SELECT kcu.column_name FROM information_schema.table_constraints AS tc JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_catalog = '{database}' AND tc.table_schema = '{schema}' AND tc.table_name = '{table}';"
        return [row[0] for row in self.execute_query(query)]
    def _fetch_table_columns(self, database, schema, table):
        query = f"SELECT column_name FROM information_schema.columns WHERE table_catalog = '{database}' AND table_schema = '{schema}' AND table_name = '{table}' ORDER BY ordinal_position;"
        return [row[0] for row in self.execute_query(query)]
    def list_tables_in_schema(self, database, schema):
        query = f"SELECT table_name FROM information_schema.tables WHERE table_catalog = '{database}' AND table_schema = '{schema}' AND table_type = 'BASE TABLE';"
        return [row[0] for row in self.execute_query(query)]
    def get_detailed_column_metadata(self, database, schema, table):
        query = f"""
        SELECT 
            c.column_name,
            c.data_type,
            c.character_maximum_length,
            c.is_nullable,
            CASE WHEN tc.constraint_type = 'PRIMARY KEY' THEN tc.constraint_name ELSE NULL END AS pk_name
        FROM information_schema.columns c
        LEFT JOIN information_schema.key_column_usage kcu
          ON c.table_catalog = kcu.table_catalog
          AND c.table_schema = kcu.table_schema
          AND c.table_name = kcu.table_name
          AND c.column_name = kcu.column_name
        LEFT JOIN information_schema.table_constraints tc
          ON kcu.constraint_name = tc.constraint_name
          AND kcu.table_schema = tc.table_schema
          AND tc.constraint_type = 'PRIMARY KEY'
        WHERE c.table_catalog = '{database}'
          AND c.table_schema = '{schema}'
          AND c.table_name = '{table}'
        ORDER BY c.ordinal_position;
        """
        cursor = self.connection.cursor()
        cursor.execute(query)
        cols = [desc[0] for desc in cursor.description]
        return [dict(zip(cols, row)) for row in cursor.fetchall()]
    def get_foreign_keys(self, database, schema, table):
        query = f"""
        SELECT
            kcu.column_name,
            ccu.table_schema AS parent_schema,
            ccu.table_name AS parent_table,
            ccu.column_name AS parent_column
        FROM information_schema.table_constraints AS tc
        JOIN information_schema.key_column_usage AS kcu
            ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage AS ccu
            ON ccu.constraint_name = tc.constraint_name AND ccu.table_schema = tc.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
            AND tc.table_catalog = '{database}'
            AND tc.table_schema = '{schema}'
            AND tc.table_name = '{table}';
        """
        cursor = self.connection.cursor()
        cursor.execute(query)
        cols = [desc[0] for desc in cursor.description]
        return [dict(zip(cols, row)) for row in cursor.fetchall()]

class MySQLHandler(BaseDBHandler):
    def connect(self):
        db_config = dict(self.config.items(self.db_key))
        self.connection = mysql.connector.connect(**db_config)
    def disconnect(self):
        if self.connection and self.connection.is_connected(): self.connection.close()
    def get_primary_keys(self, database, schema, table):
        query = f"SELECT k.COLUMN_NAME FROM information_schema.table_constraints t LEFT JOIN information_schema.key_column_usage k USING(constraint_name,table_schema,table_name) WHERE t.constraint_type='PRIMARY KEY' AND t.table_schema='{database}' AND t.table_name='{table}';"
        return [row[0] for row in self.execute_query(query)]
    def _fetch_table_columns(self, database, schema, table):
        query = f"SELECT column_name FROM information_schema.columns WHERE table_schema = '{database}' AND table_name = '{table}' ORDER BY ordinal_position;"
        return [row[0] for row in self.execute_query(query)]
    def list_tables_in_schema(self, database, schema):
        query = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{database}' AND table_type = 'BASE TABLE';"
        return [row[0] for row in self.execute_query(query)]
    def get_detailed_column_metadata(self, database, schema, table):
        query = f"""
        SELECT 
            c.COLUMN_NAME AS column_name,
            c.DATA_TYPE AS data_type,
            c.CHARACTER_MAXIMUM_LENGTH AS character_maximum_length,
            c.IS_NULLABLE AS is_nullable,
            CASE WHEN k.CONSTRAINT_NAME = 'PRIMARY' THEN 'PRIMARY KEY' ELSE NULL END as pk_name
        FROM information_schema.COLUMNS c
        LEFT JOIN information_schema.KEY_COLUMN_USAGE k 
            ON c.TABLE_SCHEMA = k.TABLE_SCHEMA 
            AND c.TABLE_NAME = k.TABLE_NAME 
            AND c.COLUMN_NAME = k.COLUMN_NAME 
            AND k.CONSTRAINT_NAME = 'PRIMARY'
        WHERE c.TABLE_SCHEMA = '{database}'
          AND c.TABLE_NAME = '{table}'
        ORDER BY c.ORDINAL_POSITION;
        """
        cursor = self.connection.cursor(dictionary=True)
        cursor.execute(query)
        return cursor.fetchall()
    def get_foreign_keys(self, database, schema, table):
        query = f"""
        SELECT
            kcu.COLUMN_NAME as column_name,
            kcu.REFERENCED_TABLE_SCHEMA AS parent_schema,
            kcu.REFERENCED_TABLE_NAME AS parent_table,
            kcu.REFERENCED_COLUMN_NAME AS parent_column
        FROM information_schema.KEY_COLUMN_USAGE AS kcu
        WHERE kcu.TABLE_SCHEMA = '{database}'
          AND kcu.TABLE_NAME = '{table}'
          AND kcu.REFERENCED_TABLE_NAME IS NOT NULL;
        """
        cursor = self.connection.cursor(dictionary=True)
        cursor.execute(query)
        return cursor.fetchall()

class RuleGenerator:
    def __init__(self, db_handler):
        self.db_handler = db_handler
        self.tc_counter = 1

    def _create_rule_dict(self, db, schema, table, col_name, scenario, expected_format, desc_suffix, extra_fields=None):
        if scenario == 'NULL':
            dimension = 'Completeness'
        elif scenario == 'UNIQUENESS':
            dimension = 'Uniqueness'
        elif scenario in ['RELATIONAL', 'REFERENTIAL_INTEGRITY']:
            dimension = 'Consistency'
        elif scenario == 'DATA TYPE':
            dimension = 'Validity'
        else:
            dimension = 'Conformity'

        rule = {
            'TC#': f'TC-{self.tc_counter:04d}',
            'Domain': schema,
            'Dimension_Matrix': dimension,
            'Test_Case': f'{scenario} check for {table}.{col_name}',
            'Description': desc_suffix,
            'Run': 'Yes',
            'Scenario': scenario,
            'Expected_Format': expected_format,
            'Target_Database': db,
            'Target_Schema': schema,
            'Target_Table': table,
            'Column_Name': col_name,
            'Allowed_Values': None,
            'Regex_Pattern': None,
            'Boundary_Min': None,
            'Boundary_Max': None,
            'whereclause': None,  # Added whereclause to the default rule dict
            'Parent_database': None,
            'Parent_schema': None,
            'Parent_table': None,
        }
        if extra_fields:
            rule.update(extra_fields)
        self.tc_counter += 1
        return rule

    def generate_for_table(self, db, schema, table):
        suggested_rules = []
        try:
            columns = self.db_handler.get_detailed_column_metadata(db, schema, table)
            foreign_keys = {fk['column_name']: fk for fk in self.db_handler.get_foreign_keys(db, schema, table)}
            full_table_name = f'"{db}"."{schema}"."{table}"'

            for col in columns:
                col_name = col['column_name']
                data_type = col.get('data_type_name', col.get('data_type', '')).lower()
                is_nullable = col.get('is_nullable', 'YES')
                pk_name = col.get('pk_name')
                char_max_len = col.get('character_maximum_length')
                is_id_col = 'id' in col_name.lower() or 'key' in col_name.lower()

                if is_nullable == 'NO':
                    suggested_rules.append(self._create_rule_dict(db, schema, table, col_name, 'NULL', 'NOT_NULL', f"Checks for NULLs in the NOT NULL column '{col_name}'."))

                if pk_name:
                    suggested_rules.append(self._create_rule_dict(db, schema, table, col_name, 'UNIQUENESS', None, f"Checks for duplicate values in the PRIMARY KEY column '{col_name}'."))

                if col_name in foreign_keys:
                    fk = foreign_keys[col_name]
                    extra = {'Scenario': 'REFERENTIAL_INTEGRITY', 'Parent_database': db, 'Parent_schema': fk['parent_schema'], 'Parent_table': fk['parent_table']}
                    suggested_rules.append(self._create_rule_dict(db, schema, table, col_name, 'RELATIONAL', 'FOREIGN_KEY_EXISTS', f"Checks that every value in '{col_name}' exists in the parent table '{fk['parent_table']}'.", extra))
                
                if not pk_name and is_id_col:
                    suggested_rules.append(self._create_rule_dict(db, schema, table, col_name, 'UNIQUENESS', None, f"Checks for duplicate values in the potential key column '{col_name}'."))

                if any(t in data_type for t in ['char', 'varchar', 'text', 'string', 'character varying']):
                    suggested_rules.append(self._create_rule_dict(db, schema, table, col_name, 'FORMAT', 'NO_WHITESPACE', f"Checks for leading/trailing whitespace in '{col_name}'."))
                    
                    if pd.notna(char_max_len) and char_max_len > 0:
                        max_len_int = int(char_max_len)
                        min_len_int = 1 if is_nullable == 'NO' else 0
                        
                        extra = {'Boundary_Min': min_len_int, 'Boundary_Max': max_len_int}
                        description = (f"Checks that '{col_name}' length is between {min_len_int} and {max_len_int} characters." 
                                       if min_len_int > 0 
                                       else f"Checks that '{col_name}' length does not exceed {max_len_int} characters.")
                        suggested_rules.append(self._create_rule_dict(db, schema, table, col_name, 'BOUNDARY', 'LENGTH', description, extra))
                    
                    if any(kw in col_name.lower() for kw in ['code', 'status', 'type', 'category', 'flag']):
                        suggested_rules.append(self._create_rule_dict(db, schema, table, col_name, 'FORMAT', 'UPPERCASE', f"Checks that values in '{col_name}' are in UPPERCASE for standardization."))
                        try:
                            distinct_query = f'SELECT DISTINCT "{col_name}" FROM {full_table_name} WHERE "{col_name}" IS NOT NULL LIMIT 21'
                            distinct_values = [str(row[0]) for row in self.db_handler.execute_query(distinct_query)]
                            if 0 < len(distinct_values) <= 20:
                                extra = {'Allowed_Values': ",".join(distinct_values)}
                                suggested_rules.append(self._create_rule_dict(db, schema, table, col_name, 'VALUE SET', None, f"Checks that '{col_name}' contains one of the discovered values.", extra))
                        except Exception as e:
                            Term.warn(f"Could not perform distinct value discovery on {col_name}. Error: {e}")

                    if any(kw in col_name.lower() for kw in ['name', 'city', 'address']):
                        suggested_rules.append(self._create_rule_dict(db, schema, table, col_name, 'FORMAT', 'NO_SPECIAL_CHARS', f"Checks for special characters in the free-text column '{col_name}'."))
                    
                    if is_id_col and not any(t in data_type for t in ['int', 'numeric', 'number']):
                         extra = {'Regex_Pattern': 'ADD_YOUR_REGEX_HERE'}
                         suggested_rules.append(self._create_rule_dict(db, schema, table, col_name, 'FORMAT', 'CUSTOM', f"Custom regex pattern for the identifier column '{col_name}'.", extra))

                if any(t in data_type for t in ['int', 'numeric', 'decimal', 'number', 'float', 'double']):
                    suggested_rules.append(self._create_rule_dict(db, schema, table, col_name, 'DATA TYPE', 'NUMERIC', f"Checks if all values in '{col_name}' are valid numerics."))

                    if any(kw in col_name.lower() for kw in ['count', 'qty', 'amount', 'price', 'quantity', 'age', 'spend', 'total']) or is_id_col:
                        suggested_rules.append(self._create_rule_dict(db, schema, table, col_name, 'BOUNDARY', 'NO_NEGATIVE', f"Checks for negative numbers in '{col_name}' which typically should be positive."))
                        if any(kw in col_name.lower() for kw in ['price', 'amount', 'spend', 'total']):
                            outlier_sql = f"SELECT * FROM {full_table_name} WHERE \"{col_name}\" > (SELECT AVG(\"{col_name}\") + (3 * STDDEV(\"{col_name}\")) FROM {full_table_name})"
                            extra = {'Allowed_Values': outlier_sql}
                            suggested_rules.append(self._create_rule_dict(db, schema, table, col_name, 'CUSTOM SQL', 'BIG_QUERIES', f"Checks for statistical outliers (more than 3 std dev from avg) in '{col_name}'.", extra))

                if any(t in data_type for t in ['date', 'timestamp']):
                    suggested_rules.append(self._create_rule_dict(db, schema, table, col_name, 'DATE', 'NOT_IN_FUTURE', f"Checks that dates in '{col_name}' are not in the future."))
                    extra = {'Allowed_Values': 'YYYY-MM-DD'}
                    suggested_rules.append(self._create_rule_dict(db, schema, table, col_name, 'DATE', 'FORMAT', f"Checks if dates in '{col_name}' conform to the 'YYYY-MM-DD' format.", extra))

                if 'email' in col_name.lower():
                    suggested_rules.append(self._create_rule_dict(db, schema, table, col_name, 'FORMAT', 'EMAIL', f"Checks for valid email formats in '{col_name}'."))
                elif 'phone' in col_name.lower():
                    suggested_rules.append(self._create_rule_dict(db, schema, table, col_name, 'FORMAT', 'PHONE', f"Checks for valid phone number formats in '{col_name}'."))

            date_cols = [c['column_name'] for c in columns if any(t in c.get('data_type','').lower() for t in ['date', 'timestamp'])]
            if 'first_name' in [c['column_name'].lower() for c in columns] and 'last_name' in [c['column_name'].lower() for c in columns]:
                combined_cols = "FIRST_NAME,LAST_NAME"
                suggested_rules.append(self._create_rule_dict(db, schema, table, combined_cols, 'UNIQUENESS', None, f"Checks for duplicate combinations of FIRST_NAME and LAST_NAME."))

            for start_col in date_cols:
                if 'start' in start_col.lower() or 'begin' in start_col.lower() or 'order' in start_col.lower():
                    prefix = start_col.lower().replace('start','').replace('begin','').replace('order','')
                    for end_col in date_cols:
                        if ('end' in end_col.lower() or 'ship' in end_col.lower() or 'delivery' in end_col.lower()) and prefix in end_col.lower():
                            combined_cols = f"{end_col},{start_col}"
                            suggested_rules.append(self._create_rule_dict(db, schema, table, combined_cols, 'DATE', 'IS_AFTER', f"Checks that '{end_col}' is on or after '{start_col}'."))

        except Exception:
            Term.warn(f"Could not generate rules for '{db}.{schema}.{table}'. An exception occurred:")
            tb_str = traceback.format_exc()
            print(f"{Fore.RED}{tb_str}{Style.RESET_ALL}")
            Term._log(f"TRACEBACK for {db}.{schema}.{table}:\n{tb_str}")
        
        return suggested_rules

def write_suggestions_to_excel(df: pd.DataFrame, config):
    output_dir = Path(config.get('OUTPUT', 'output_directory', fallback='.'))
    suggestion_filename = config.get('SETTINGS', 'Suggestion_Output_Filename', fallback='Suggested_Rules.xlsx')

    output_dir.mkdir(parents=True, exist_ok=True)
    
    filepath = output_dir / suggestion_filename

    Term.info(f"Writing {len(df)} suggested rules to a new file: {filepath}...")
    
    all_headers = [
        'TC#', 'Domain', 'Dimension_Matrix', 'Test_Case', 'Description', 'Run', 
        'Scenario', 'Expected_Format', 'Target_Database', 'Target_Schema', 
        'Target_Table', 'Column_Name', 'Allowed_Values', 'Regex_Pattern', 
        'Boundary_Min', 'Boundary_Max', 'whereclause', 'Parent_database',  # Added whereclause
        'Parent_schema', 'Parent_table'
    ]
    df = df.reindex(columns=all_headers)

    try:
        with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Suggestion_Rules', index=False)
            worksheet = writer.sheets['Suggestion_Rules']
            for column in worksheet.columns:
                max_length = 0
                column_letter = column[0].column_letter
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(cell.value)
                    except:
                        pass
                adjusted_width = (max_length + 2)
                worksheet.column_dimensions[column_letter].width = adjusted_width

        Term.info(f"Successfully wrote suggestions to '{filepath}'.")
        Term.info("Please review this file, then copy and paste desired rules into your 'DQ_Scenarios.xlsx' Scenario sheet.")

    except Exception as e:
        Term.fatal(f"Failed to write suggestion file to '{filepath}': {e}")

def get_db_handler(config):
    db_type = config.get('DATABASE', 'selected_db')
    db_key_map = {'snowflake': 'SNOWFLAKE', 'postgres': 'POSTGRES', 'auroradb_postgres': 'AURORADB_POSTGRES', 'mysql': 'MYSQL', 'auroradb_mysql': 'AURORADB_MYSQL'}
    if db_type not in db_key_map: raise ValueError(f"Unsupported database type in config: {db_type}")
    db_key = db_key_map[db_type]
    if db_type == 'snowflake': return SnowflakeHandler(config, db_key)
    elif 'postgres' in db_type: return PostgresHandler(config, db_key)
    elif 'mysql' in db_type: return MySQLHandler(config, db_key)
    else: raise ValueError(f"Unsupported database type: {db_type}")

class DataProfiler:
    def __init__(self, db_handler):
        self.db_handler = db_handler

    def profile_table(self, db, schema, table):
        Term.info(f"Profiling table: {db}.{schema}.{table}")
        full_table_name = f'"{db}"."{schema}"."{table}"'
        try:
            columns_metadata = self.db_handler.get_detailed_column_metadata(db, schema, table)
            if not columns_metadata:
                Term.warn(f"Could not retrieve column metadata for {table}. Skipping profiling.")
                return None

            row_count_query = f"SELECT COUNT(*) FROM {full_table_name}"
            row_count = self.db_handler.execute_query(row_count_query)[0][0]
            table_stats = {'row_count': row_count, 'column_count': len(columns_metadata)}
            
            column_stats = []
            for col_meta in tqdm(columns_metadata, desc=f"Profiling {table}", unit="col"):
                col_name = col_meta['column_name']
                col_name_sql = f'"{col_name}"'
                data_type = col_meta.get('data_type', 'N/A')
                
                stats = {'column_name': col_name, 'data_type': data_type}
                
                base_queries = {
                    'null_count': f"SELECT COUNT(*) FROM {full_table_name} WHERE {col_name_sql} IS NULL",
                    'distinct_count': f"SELECT COUNT(DISTINCT {col_name_sql}) FROM {full_table_name}"
                }
                
                try:
                    stats['null_count'] = self.db_handler.execute_query(base_queries['null_count'])[0][0]
                    stats['distinct_count'] = self.db_handler.execute_query(base_queries['distinct_count'])[0][0]
                    stats['null_percentage'] = (stats['null_count'] / row_count * 100) if row_count > 0 else 0
                    stats['distinct_percentage'] = (stats['distinct_count'] / row_count * 100) if row_count > 0 else 0
                except Exception as e:
                    Term.warn(f"Could not compute base stats for {table}.{col_name}: {e}")
                    continue

                if any(t in str(data_type).lower() for t in ['int', 'numeric', 'decimal', 'number', 'float', 'double', 'fixed']):
                    numeric_query = f"SELECT MIN({col_name_sql}), MAX({col_name_sql}), AVG({col_name_sql}), STDDEV({col_name_sql}) FROM {full_table_name}"
                    try:
                        num_results = self.db_handler.execute_query(numeric_query)[0]
                        stats.update({'min': num_results[0], 'max': num_results[1], 'avg': num_results[2], 'std_dev': num_results[3]})
                    except Exception: pass

                if any(t in str(data_type).lower() for t in ['char', 'varchar', 'text', 'string']):
                    len_query = f"SELECT MIN(LENGTH(CAST({col_name_sql} AS VARCHAR))), MAX(LENGTH(CAST({col_name_sql} AS VARCHAR))), AVG(LENGTH(CAST({col_name_sql} AS VARCHAR))) FROM {full_table_name} WHERE {col_name_sql} IS NOT NULL"
                    try:
                        len_results = self.db_handler.execute_query(len_query)[0]
                        stats.update({'min_len': len_results[0], 'max_len': len_results[1], 'avg_len': len_results[2]})
                    except Exception: pass

                column_stats.append(stats)
            
            return {'table_name': f"{db}.{schema}.{table}", 'table_stats': table_stats, 'column_stats': column_stats}
        except Exception as e:
            Term.warn(f"Failed to profile table {full_table_name}. Error: {e}")
            return None
CHECK_TIMEOUT_SECONDS = 600

def main():
    parser = argparse.ArgumentParser(description="Data Quality Validation Tool.")
    args = parser.parse_args()

    Term.initialize_logging()
    os.system('cls' if os.name == 'nt' else 'clear')
    warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy connectable.*")
    
    config = configparser.ConfigParser()
    if not Path('config.ini').exists():
        Term.fatal("config.ini not found. Please ensure the file exists.")
    config.read('config.ini')

    generate_scenarios_mode = config.getboolean('SETTINGS', 'Generate_Scenarios', fallback=False)
    enable_profiling = config.getboolean('SETTINGS', 'Enable_Profiling', fallback=True)
    enable_visual_report = config.getboolean('SETTINGS', 'Enable_Visual_Report', fallback=True)
    enable_scenario_report = config.getboolean('SETTINGS', 'Enable_Scenario_Report', fallback=True)
    enable_defects_tab = config.getboolean('SETTINGS', 'Enable_Defects_Tab', fallback=True)
    enable_defect_flow = config.getboolean('SETTINGS', 'Enable_Defect_Flow', fallback=True)

    if generate_scenarios_mode:
        Term.header("AUTOMATED RULE GENERATION MODE")
        db_handler = None
        try:
            db_handler = get_db_handler(config)
            db_handler.connect()
            Term.info("Successfully connected to the database for metadata discovery.")
            try:
                tables_df = pd.read_excel('DQ_Scenarios.xlsx', sheet_name='Tables')
                if not all(col in tables_df.columns for col in ['Database', 'Schema', 'Table']):
                     Term.fatal("The 'Tables' sheet must contain 'Database', 'Schema', and 'Table' columns.")
            except FileNotFoundError:
                Term.fatal("DQ_Scenarios.xlsx not found in the script directory.")
            except ValueError:
                Term.fatal("A sheet named 'Tables' was not found in DQ_Scenarios.xlsx.")
            generator = RuleGenerator(db_handler)
            all_suggested_rules = []
            Term.info(f"Scanning {len(tables_df)} tables listed in the 'Tables' sheet...")
            for index, row in tqdm(tables_df.iterrows(), total=len(tables_df), desc="Scanning Tables"):
                db, schema, table = row['Database'], row['Schema'], row['Table']
                rules = generator.generate_for_table(db, schema, table)
                all_suggested_rules.extend(rules)
            if not all_suggested_rules:
                Term.warn("No suggested rules were generated. Check if the tables exist and have columns.")
            else:
                output_df = pd.DataFrame(all_suggested_rules)
                write_suggestions_to_excel(output_df, config)
        except Exception as e:
            Term.fatal(f"Rule generation failed: {e}\n{traceback.format_exc()}")
        finally:
            if db_handler:
                db_handler.disconnect()
        Term.header("RULE GENERATION FINISHED SUCCESSFULLY")
        sys.exit(0)
    
    Term.header("STARTING DATA VALIDATION PROCESS")
    
    Term.info("Loading orchestration rules from 'Run' sheet in DQ_Scenarios.xlsx...")
    try:
        xls = pd.ExcelFile('DQ_Scenarios.xlsx')
        run_sheet_df = pd.read_excel(xls, sheet_name='Run', dtype=str)
    except FileNotFoundError:
        Term.fatal("DQ_Scenarios.xlsx not found in the script directory.")
    except ValueError:
        Term.fatal("A sheet named 'Run' was not found in DQ_Scenarios.xlsx. This sheet is required to orchestrate test execution.")

    runnable_tcs = run_sheet_df[run_sheet_df['Run'].astype(str).str.lower() == 'yes'].copy()

    if runnable_tcs.empty:
        Term.warn("No Test Cases marked to run in the 'Run' sheet. Exiting.")
        sys.exit(0)

    Term.info(f"Found {len(runnable_tcs)} Test Cases to execute: {runnable_tcs['TC'].tolist()}")
    
    db_type = config.get('DATABASE', 'selected_db', fallback='N/A')
    Term.header(f"CONNECTING TO DATABASE: {db_type.upper()}")
    db_handler = None
    try:
        db_handler = get_db_handler(config)
        db_handler.connect()
        Term.info(f"Successfully connected to {db_type}.")
    except Exception as e:
        Term.fatal(f"Failed to connect to the database: {e}")

    profile_df_full = None
    try:
        if 'Tables' in xls.sheet_names:
            profile_df_full = pd.read_excel(xls, sheet_name='Tables')
            if 'TC Name' in profile_df_full.columns:
                 profile_df_full.rename(columns={'TC Name': 'TC_Name'}, inplace=True)

    except Exception as e:
        Term.warn(f"Could not load the 'Tables' sheet for profiling. Profiling will be skipped. Error: {e}")


    for _, tc_row in runnable_tcs.iterrows():
        tc_name = tc_row.get('TC')
        tc_description = tc_row.get('Description', 'NoDescription')

        if not tc_name:
            Term.warn("Found a row in 'Run' sheet marked 'Yes' but with an empty 'TC' name. Skipping.")
            continue
        
        safe_description = re.sub(r'[<>:"/\\|?*]', '_', str(tc_description))

        Term.header(f"EXECUTING TEST CASE: {tc_name} ({tc_description})")

        try:
            # Ensure dtype=str to keep the whereclause as a string, even if it looks like a number
            rules_df = pd.read_excel(xls, sheet_name=tc_name, engine='openpyxl', dtype=str)
            rules_df['__index__'] = rules_df.index
            rules_df.columns = [c.replace(' ', '_') for c in rules_df.columns]
            Term.info(f"Loaded {len(rules_df)} total validation rules from sheet '{tc_name}'.")
        except ValueError:
            Term.warn(f"Sheet named '{tc_name}' not found in DQ_Scenarios.xlsx. Skipping this test case.")
            continue
        
        if 'Run' in rules_df.columns:
            initial_count = len(rules_df)
            rules_df = rules_df[rules_df['Run'].astype(str).str.lower() == 'yes'].copy()
            Term.info(f"Filtered to {len(rules_df)} rules marked to be executed (out of {initial_count} total).")
        else:
            Term.warn(f"No 'Run' column found in sheet '{tc_name}'; running all scenarios in this sheet by default.")

        if rules_df.empty:
            Term.warn(f"No scenarios to run for Test Case '{tc_name}'. Moving to the next.")
            continue

        all_results, pk_results = [], []
        Term.header("PRIMARY KEY DISCOVERY")
        try:
            unique_tables = rules_df[['Target_Database', 'Target_Schema', 'Target_Table']].drop_duplicates().dropna()
            Term.info(f"Discovering primary keys for {len(unique_tables)} unique tables...")
            for _, t in tqdm(unique_tables.iterrows(), total=len(unique_tables), desc="Scanning Tables"):
                keys = db_handler.get_primary_keys(t['Target_Database'], t['Target_Schema'], t['Target_Table'])
                pk_results.append({'Database': t['Target_Database'], 'Schema': t['Target_Schema'], 'Table': t['Target_Table'], 'PrimaryKeys': ', '.join(map(str, keys)) if keys else 'N/A'})
            Term.info("Primary key discovery complete.")
        except Exception as e:
            Term.warn(f"Could not complete primary key discovery for {tc_name}. Error: {e}")

        profiling_results = []
        if enable_profiling:
            Term.header("DATA PROFILING")
            if profile_df_full is not None:
                if 'TC_Name' in profile_df_full.columns:
                    profile_df_filtered = profile_df_full[profile_df_full['TC_Name'] == tc_name]

                    if not profile_df_filtered.empty:
                        profiler = DataProfiler(db_handler)
                        required_cols = ['Database', 'Schema', 'Table']
                        unique_tables_to_profile = profile_df_filtered[required_cols].drop_duplicates().dropna()
                        
                        if not unique_tables_to_profile.empty:
                            Term.info(f"Found {len(unique_tables_to_profile)} table(s) to profile for '{tc_name}'.")
                            for _, row in unique_tables_to_profile.iterrows():
                                result = profiler.profile_table(row['Database'], row['Schema'], row['Table'])
                                if result:
                                    profiling_results.append(result)
                        else:
                            Term.info(f"No valid table entries found for '{tc_name}' in the 'Tables' sheet.")
                    else:
                        Term.info(f"No tables in the 'Tables' sheet are marked for profiling under TC_Name '{tc_name}'.")
                else:
                    Term.warn("Profiling is enabled, but the 'Tables' sheet is missing the required 'TC_Name' column. Skipping profiling.")
            else:
                Term.info("No 'Tables' sheet found. Skipping data profiling.")
        else:
            Term.info("Data Profiling is disabled in config.ini. Skipping.")


        Term.header(f"EXECUTING {len(rules_df)} VALIDATION CHECKS FOR {tc_name}")
        print(f"(Timeout set to {CHECK_TIMEOUT_SECONDS}s per check)")
        engine = ValidationEngine(db_handler, config)
        
        results_by_tc = {}
        pending_rules = rules_df.copy()
        max_iterations = len(pending_rules) + 1

        for i in range(max_iterations):
            if pending_rules.empty:
                break
            
            runnable_mask = pending_rules.apply(
                lambda row: (
                    pd.isna(row.get('Depends_On_TC#')) or str(row.get('Depends_On_TC#')).strip() == '' or
                    (
                        str(row.get('Depends_On_TC#')).strip() in results_by_tc and
                        results_by_tc[str(row.get('Depends_On_TC#')).strip()]['Status'].upper() in 
                        [s.strip().upper() for s in str(row.get('Required_Status', 'PASS')).split(',')]
                    )
                ), axis=1
            )
            
            runnable_df = pending_rules[runnable_mask]
            
            if runnable_df.empty and not pending_rules.empty:
                Term.warn(f"Stopping execution: {len(pending_rules)} rules remain with unmet or circular dependencies.")
                for _, rule in pending_rules.iterrows():
                    msg = f"Skipped: Prerequisite TC '{rule['Depends_On_TC#']}' was not met or resulted in a non-required status."
                    all_results.append(engine._format_result(rule, "SKIPPED", 0, msg, None, [rule.get('Column_Name')], None, 0))
                break

            pending_rules = pending_rules[~runnable_mask]
            
            grouped_scenarios = runnable_df.groupby('Scenario')

            for scenario_name, scenario_group_df in grouped_scenarios:
                num_checks = len(scenario_group_df)
                Term.scenario_header(f"Scenario: {scenario_name} ({num_checks} check(s))")

                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future_to_rule = {executor.submit(engine.run_check, row): row for _, row in scenario_group_df.iterrows()}
                    
                    progress_bar = tqdm(
                        concurrent.futures.as_completed(future_to_rule), 
                        total=num_checks, 
                        desc=f"{scenario_name:<25}"
                    )

                    for future in progress_bar:
                        rule = future_to_rule[future]
                        try:
                            result = future.result(timeout=CHECK_TIMEOUT_SECONDS)
                        except concurrent.futures.TimeoutError:
                            result = engine._format_result(rule, "TIMEOUT", 0, f"Check exceeded {CHECK_TIMEOUT_SECONDS}s limit.", None, [rule.get('Column_Name')], None, 0)
                        except Exception as exc:
                            result = engine._format_result(rule, "ERROR", 0, str(exc), None, [rule.get('Column_Name')], None, 0)
                        
                        all_results.append(result)
                        if 'TC#' in result and pd.notna(result['TC#']):
                            results_by_tc[str(result['TC#']).strip()] = result
        
        Term.info("All checks for this test case completed or timed out.")

        if not all_results:
            Term.warn(f"No results were generated for Test Case '{tc_name}'. Skipping report generation.")
            continue
            
        base_output_dir = config.get('OUTPUT', 'output_directory', fallback='.')
        dynamic_output_dir = os.path.join(base_output_dir, safe_description)
        Term.info(f"Report output directory for this run: {dynamic_output_dir}")

        sorted_results = sorted(all_results, key=lambda x: x.get('__index__', 0))
        reporter = ReportGenerator(
            sorted_results, 
            pk_results, 
            config, 
            profiling_results, 
            profiling_enabled=enable_profiling,
            dynamic_output_dir=dynamic_output_dir,
            tc_name=tc_name,
            enable_visual_report=enable_visual_report,
            enable_scenario_report=enable_scenario_report,
            enable_defects_tab=enable_defects_tab,
            enable_defect_flow=enable_defect_flow
        )
        excel_filepath, html_filepath = reporter.generate_reports()

        if excel_filepath and config.get('OUTPUT', 'auto_open_excel', fallback='yes').lower() == 'yes':
            Term.info(f"Opening Excel report for {tc_name}...")
            try:
                os.startfile(excel_filepath)
            except AttributeError:
                import subprocess
                opener = "open" if sys.platform == "darwin" else "xdg-open"
                subprocess.call([opener, excel_filepath])
        if html_filepath and config.get('OUTPUT', 'auto_open_html', fallback='yes').lower() == 'yes':
            Term.info(f"Opening HTML report for {tc_name} in browser...")
            cache_buster = int(time())
            webbrowser.open(f'file://{os.path.realpath(html_filepath)}?v={cache_buster}')
    
    if db_handler:
        db_handler.disconnect()
        Term.info("Database connection closed.")

    Term.header("ALL TEST CASES FINISHED SUCCESSFULLY")
    Term.close_log()

if __name__ == '__main__':
    main()
