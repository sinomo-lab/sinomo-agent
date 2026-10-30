; cargo-packager renders paths and metadata; Sinomo owns initial setup only.
Unicode true
!include MUI2.nsh
!include nsDialogs.nsh
!include LogicLib.nsh
!include FileFunc.nsh

Name "Sinomo"
OutFile "{{out_file}}"
RequestExecutionLevel user
InstallDir "$LOCALAPPDATA\Programs\Sinomo"
SetCompressor /SOLID lzma
!define UNINSTALL_KEY "Software\Microsoft\Windows\CurrentVersion\Uninstall\Sinomo"
Var DataDir
Var DataInput
Var Dialog
Var Browse
Var PathFlag
Var MenuFlag
Var DesktopFlag
Var DeleteData
Var DeleteCheck
Var SkipMenu
Var UiFlag
Var StartFlag

!insertmacro MUI_PAGE_WELCOME
!define MUI_DIRECTORYPAGE_TEXT_TOP "Choose the folder for setup and uninstall files. Sinomo program files will live inside the data folder selected on the next page."
!insertmacro MUI_PAGE_DIRECTORY
Page custom DataPage DataLeave
!insertmacro MUI_PAGE_COMPONENTS
!insertmacro MUI_PAGE_INSTFILES
!insertmacro MUI_PAGE_FINISH
!insertmacro MUI_UNPAGE_CONFIRM
UninstPage custom un.DataPage un.DataLeave
!insertmacro MUI_UNPAGE_INSTFILES
!insertmacro MUI_LANGUAGE "English"

Function .onInit
  SetShellVarContext current
  ReadEnvStr $DataDir "SINOMO_DATA_DIR"
  ${If} $DataDir == ""
    StrCpy $DataDir "$PROFILE\.sinomo"
  ${EndIf}
  StrCpy $PathFlag ""
  StrCpy $MenuFlag ""
  StrCpy $DesktopFlag ""
  StrCpy $SkipMenu "0"
  StrCpy $UiFlag ""
  StrCpy $StartFlag ""
  IfSilent 0 +2
    StrCpy $UiFlag "--no-dialogs"
  ${GetParameters} $0
  ClearErrors
  ${GetOptions} $0 "/NO_START" $1
  ${IfNot} ${Errors}
    StrCpy $StartFlag "--no-start"
  ${EndIf}
  ClearErrors
  ${GetOptions} $0 "/DATA_DIR=" $1
  ${IfNot} ${Errors}
    StrCpy $DataDir $1
  ${EndIf}
  ClearErrors
  ${GetOptions} $0 "/NO_MENU" $1
  ${IfNot} ${Errors}
    StrCpy $SkipMenu "1"
  ${EndIf}
  IfSilent 0 automation_done
  ClearErrors
  ${GetOptions} $0 "/ADD_PATH" $1
  ${IfNot} ${Errors}
    StrCpy $PathFlag "--path"
  ${EndIf}
  automation_done:
FunctionEnd

Function .onVerifyInstDir
  Call CheckEmptyDestination
FunctionEnd

Function CheckEmptyDestination
  ; Never repair or upgrade by overwriting a possibly evolved bootstrap.
  IfFileExists "$INSTDIR\*.*" 0 done
  FindFirst $0 $1 "$INSTDIR\*.*"
  loop:
    StrCmp $1 "" empty
    StrCmp $1 "." next
    StrCmp $1 ".." next
    FindClose $0
    SetErrorLevel 1
    Abort
  next:
    FindNext $0 $1
    Goto loop
  empty:
    FindClose $0
  done:
FunctionEnd

Function DataPage
  !insertmacro MUI_HEADER_TEXT "Program and data directory" "Choose where Sinomo stores its program files, cluster state and data."
  nsDialogs::Create 1018
  Pop $Dialog
  ${NSD_CreateLabel} 0 0 100% 30u "Choose a separate folder for the program and its data. Existing evolved components and data are preserved."
  Pop $0
  ${NSD_CreateDirRequest} 0 42u 78% 12u "$DataDir"
  Pop $DataInput
  ${NSD_CreateBrowseButton} 80% 40u 20% 16u "Browse..."
  Pop $Browse
  ${NSD_OnClick} $Browse BrowseData
  nsDialogs::Show
FunctionEnd

Function BrowseData
  nsDialogs::SelectFolderDialog "Sinomo data directory" "$DataDir"
  Pop $0
  StrCmp $0 "error" done
  ${NSD_SetText} $DataInput $0
  done:
FunctionEnd

Function DataLeave
  ${NSD_GetText} $DataInput $DataDir
  StrCmp $DataDir "" invalid
  StrCmp $DataDir $INSTDIR invalid
  Return
  invalid:
    MessageBox MB_OK|MB_ICONEXCLAMATION "Choose a data directory separate from the installation folder."
    Abort
FunctionEnd

Section "Sinomo CLI and app (required)" Main
  SectionIn RO
  Call CheckEmptyDestination
  ReadRegStr $0 HKCU "${UNINSTALL_KEY}" "InstallLocation"
  ${If} $0 != ""
    MessageBox MB_OK|MB_ICONSTOP "Sinomo is already registered at $0. Uninstall it before installing another copy. Existing cluster data can be retained." /SD IDOK
    SetErrorLevel 1
    Abort
  ${EndIf}
  SetOutPath "$INSTDIR"
  File "{{main_binary_path}}"
  {{#each binaries}}
  File /a "/oname={{this}}" "{{@key}}"
  {{/each}}
  {{#each resources}}
  File /a "/oname={{this}}" "{{@key}}"
  {{/each}}
SectionEnd

Section /o "Add installation directory to user PATH" PathSection
  StrCpy $PathFlag "--path"
SectionEnd
Section /o "Start Menu shortcut" MenuSection
  ${If} $SkipMenu != "1"
    StrCpy $MenuFlag "--menu"
  ${EndIf}
SectionEnd
Section /o "Desktop shortcut" DesktopSection
  StrCpy $DesktopFlag "--desktop"
SectionEnd

Section "-Configure"
  WriteUninstaller "$INSTDIR\Uninstall.exe"
  WriteRegStr HKCU "${UNINSTALL_KEY}" "DisplayName" "Sinomo"
  WriteRegStr HKCU "${UNINSTALL_KEY}" "DisplayVersion" "{{version}}"
  WriteRegStr HKCU "${UNINSTALL_KEY}" "Publisher" "Sinomo"
  WriteRegStr HKCU "${UNINSTALL_KEY}" "InstallLocation" "$INSTDIR"
  WriteRegStr HKCU "${UNINSTALL_KEY}" "DisplayIcon" "$DataDir\bin\sinomo-app.exe"
  WriteRegStr HKCU "${UNINSTALL_KEY}" "UninstallString" '$\"$INSTDIR\Uninstall.exe$\"'
  WriteRegStr HKCU "${UNINSTALL_KEY}" "QuietUninstallString" '$\"$INSTDIR\Uninstall.exe$\" /S'
  WriteRegDWORD HKCU "${UNINSTALL_KEY}" "NoModify" 1
  WriteRegDWORD HKCU "${UNINSTALL_KEY}" "NoRepair" 1
  ExecWait '$\"$INSTDIR\sinomo-setup.exe$\" configure --install-dir $\"$INSTDIR$\" --data-dir $\"$DataDir$\" $PathFlag $MenuFlag $DesktopFlag $UiFlag $StartFlag' $0
  ${If} $0 != 0
    IfFileExists "$INSTDIR\sinomo-install.json" retained
      ; No receipt means configuration failed before making managed-state changes.
      ; Remove only the exact extracted payload, never the chosen directory tree.
      Delete "$INSTDIR\sinomo-setup.exe"
      {{#each binaries}}
      Delete "$INSTDIR\{{this}}"
      {{/each}}
      {{#each resources}}
      Delete "$INSTDIR\{{this}}"
      {{/each}}
      Delete "$INSTDIR\Uninstall.exe"
      DeleteRegKey HKCU "${UNINSTALL_KEY}"
      RMDir "$INSTDIR"
      MessageBox MB_OK|MB_ICONSTOP "Sinomo setup did not finish. Review the setup error and choose valid, separate folders." /SD IDOK
      Goto failed
    retained:
      MessageBox MB_OK|MB_ICONSTOP "Sinomo setup did not finish. Review the setup error. The registered uninstaller can remove a partially configured installation." /SD IDOK
    failed:
    SetErrorLevel 1
    Abort
  ${EndIf}
SectionEnd

Function un.onInit
  SetShellVarContext current
  StrCpy $DeleteData ""
  StrCpy $UiFlag ""
  IfSilent 0 +2
    StrCpy $UiFlag "--no-dialogs"
  ${GetParameters} $0
  ClearErrors
  ${GetOptions} $0 "/DELETE_DATA" $1
  ${IfNot} ${Errors}
    StrCpy $DeleteData "--delete-data"
  ${EndIf}
FunctionEnd

Function un.DataPage
  nsDialogs::Create 1018
  Pop $Dialog
  ${NSD_CreateLabel} 0 0 100% 35u "Keep your Sinomo data to reinstall later, or explicitly delete it. Deletion is refused if the data folder belongs to another installation."
  Pop $0
  ${NSD_CreateCheckbox} 0 45u 100% 20u "Permanently delete the installation's data directory"
  Pop $DeleteCheck
  ; Unchecked by default, including silent uninstall.
  nsDialogs::Show
FunctionEnd

Function un.DataLeave
  ${NSD_GetState} $DeleteCheck $0
  ${If} $0 == ${BST_CHECKED}
    MessageBox MB_YESNO|MB_DEFBUTTON2|MB_ICONEXCLAMATION "Delete all cluster state, credentials and files in the selected data directory permanently?" IDNO keep
    StrCpy $DeleteData "--delete-data"
  ${EndIf}
  keep:
FunctionEnd

Section "Uninstall"
  ExecWait '$\"$INSTDIR\sinomo-setup.exe$\" uninstall --install-dir $\"$INSTDIR$\" $DeleteData $UiFlag' $0
  ${If} $0 != 0
    MessageBox MB_OK|MB_ICONSTOP "Uninstall stopped. Close the app or resolve the displayed error and retry; the uninstaller remains registered." /SD IDOK
    SetErrorLevel 1
    Abort
  ${EndIf}
  Delete "$INSTDIR\sinomo-setup.exe"
  Delete "$INSTDIR\Uninstall.exe"
  RMDir "$INSTDIR"
  DeleteRegKey HKCU "${UNINSTALL_KEY}"
SectionEnd
