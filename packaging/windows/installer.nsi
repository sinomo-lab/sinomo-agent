; The package only extracts a verified distribution and invokes the shared setup
; frontend. Rust owns installation identity, settings and uninstall registration.
Unicode true
!include LogicLib.nsh
!include FileFunc.nsh
Name "Sinomo"
Icon "{{installer_icon}}"
OutFile "{{out_file}}"
RequestExecutionLevel user
InstallDir "$LOCALAPPDATA\Programs\Sinomo"
SetCompressor /SOLID lzma
Var SetupFlags
Var UiFlag
Var RootFlag
Var DataFlag
Page instfiles
Function .onInit
  SetShellVarContext current
  InitPluginsDir
  StrCpy $SetupFlags ""
  StrCpy $UiFlag "--interactive"
  IfSilent 0 +2
    StrCpy $UiFlag "--no-dialogs"
  StrCpy $RootFlag ""
  ${If} $INSTDIR != "$LOCALAPPDATA\Programs\Sinomo"
    StrCpy $RootFlag '--install-dir $\"$INSTDIR$\"'
  ${EndIf}
  StrCpy $DataFlag ""
  ${GetParameters} $0
  ClearErrors
  ${GetOptions} $0 "/REPAIR" $1
  ${IfNot} ${Errors}
    ${If} $1 == ""
      StrCpy $SetupFlags "$SetupFlags --repair"
    ${EndIf}
  ${EndIf}
  ClearErrors
  ${GetOptions} $0 "/DATA_DIR=" $1
  ${IfNot} ${Errors}
    StrCpy $DataFlag '--data-dir $\"$1$\"'
  ${EndIf}
  ClearErrors
  ${GetOptions} $0 "/ADD_PATH" $1
  ${IfNot} ${Errors}
    StrCpy $SetupFlags "$SetupFlags --add-to-path"
  ${EndIf}
  ClearErrors
  ${GetOptions} $0 "/NO_PATH" $1
  ${IfNot} ${Errors}
    StrCpy $SetupFlags "$SetupFlags --no-add-to-path"
  ${EndIf}
  ClearErrors
  ${GetOptions} $0 "/NO_MENU" $1
  ${IfNot} ${Errors}
    StrCpy $SetupFlags "$SetupFlags --no-menu-shortcut"
  ${EndIf}
  ClearErrors
  ${GetOptions} $0 "/ADD_MENU" $1
  ${IfNot} ${Errors}
    StrCpy $SetupFlags "$SetupFlags --menu-shortcut"
  ${EndIf}
  ClearErrors
  ${GetOptions} $0 "/ADD_DESKTOP" $1
  ${IfNot} ${Errors}
    StrCpy $SetupFlags "$SetupFlags --desktop-shortcut"
  ${EndIf}
  ClearErrors
  ${GetOptions} $0 "/NO_DESKTOP" $1
  ${IfNot} ${Errors}
    StrCpy $SetupFlags "$SetupFlags --no-desktop-shortcut"
  ${EndIf}
  ClearErrors
  ${GetOptions} $0 "/NO_STARTUP" $1
  ${IfNot} ${Errors}
    StrCpy $SetupFlags "$SetupFlags --no-startup"
  ${EndIf}
  ClearErrors
  ${GetOptions} $0 "/STARTUP" $1
  ${IfNot} ${Errors}
    StrCpy $SetupFlags "$SetupFlags --startup"
  ${EndIf}
  ClearErrors
  ${GetOptions} $0 "/NO_START" $1
  ${IfNot} ${Errors}
    ${If} $1 == ""
      StrCpy $SetupFlags "$SetupFlags --no-start"
    ${EndIf}
  ${EndIf}
FunctionEnd
Section "Sinomo setup"
  SetOutPath "$PLUGINSDIR\Sinomo"
  File "{{main_binary_path}}"
  {{#each binaries}}
  File /a "/oname={{this}}" "{{@key}}"
  {{/each}}
  {{#each resources}}
  File /a "/oname={{this}}" "{{@key}}"
  {{/each}}
  ExecWait '$\"$PLUGINSDIR\Sinomo\sinomo-setup.exe$\" install $RootFlag $DataFlag $SetupFlags $UiFlag' $0
  ${If} $0 != 0
    SetErrorLevel 1
    Abort
  ${EndIf}
SectionEnd
