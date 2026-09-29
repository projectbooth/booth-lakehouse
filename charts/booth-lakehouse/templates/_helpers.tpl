{{/* Standard name/label helpers, the same shape every booth-* chart uses. Components: api, lakekeeper. */}}

{{- define "booth-lakehouse.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "booth-lakehouse.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "booth-lakehouse.labels" -}}
app.kubernetes.io/name: {{ include "booth-lakehouse.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
booth.projectbooth.io/module: lakehouse
{{- end -}}

{{- define "booth-lakehouse.selectorLabels" -}}
app.kubernetes.io/name: {{ include "booth-lakehouse.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "booth-lakehouse.brokerUrl" -}}
{{- default (printf "%s/api/credentials" (trimSuffix "/" .Values.core.url)) .Values.broker.url -}}
{{- end -}}
